from datetime import date
import re
import requests
import serial
import json
import threading
from serial.tools import list_ports
import time
from flask import Response, redirect, render_template, request, session, url_for, jsonify, flash, abort
from app.auth.routes import login_required
from app.main import main_bp
from app.services.api_service import APIService
from app.services.sqlite_service import SQLiteService
from app.utils.logger import log_error
from app.utils.sincronizador import Sincronizador
from config import Config

def buscar_puerto_por_numero_serie(serial_objetivo):
    """Devuelve el puerto COM asignado a un número de serie USB específico."""
    for port in list_ports.comports():
        if port.serial_number == serial_objetivo:
            return port.device
    return None

@main_bp.route('/')
@login_required # Esta ruta requiere que el usuario esté logueado
def work():
    """
    Página de trabajo para usuarios autenticados.
    """
    nomina = session.get('username')
    fecha_hoy = date.today().isoformat()
    params = request.args.to_dict()
    return render_template('main/work.html', fecha_hoy=fecha_hoy, params=params, nomina=nomina)

@main_bp.route('/reportes')
@login_required # Esta ruta requiere que el usuario esté logueado
def reportes():
    """
    Página de reportes.
    """
    nomina = session.get('username')
    rol = session.get('role')
    fecha_hoy = date.today().isoformat()
    params = request.args.to_dict()
    return render_template('main/reportes.html', fecha_hoy=fecha_hoy, params=params, nomina=nomina, rol=rol)

@main_bp.route('/toda_la_data_de_local')
@login_required # Esta ruta requiere que el usuario esté logueado
def toda_la_data_de_local():
    sqliteService = SQLiteService()
    data = sqliteService.obtener_reportes()
    return jsonify(data)

@main_bp.route('/manual')
@login_required # Esta ruta requiere que el usuario esté logueado
def manual():
    """
    Página de manual.
    """
    username = session.get('username')
    return render_template('main/manual.html',)

@main_bp.route('/peso_bruto')
@login_required
def peso_bruto():
    with open(Config.LOCAL_CONFIGS, 'r') as f:
        MAPA_BASCULAS = json.load(f)["mapa_basculas"]
    bascula_id = request.args.get('bascula', '').strip()
    if bascula_id not in MAPA_BASCULAS:
        return jsonify({"error": "Bascula incorrecta o no registrada"}), 400
    
    numero_serie_objetivo = MAPA_BASCULAS[bascula_id]
    puerto = buscar_puerto_por_numero_serie(numero_serie_objetivo)

    try:
        # Abrir puerto serial
        with serial.Serial(puerto, baudrate=9600, timeout=1) as ser:
            ser.write(b'P\r\n')
            raw = ser.readline().decode('utf-8').strip()

        # Si no hay datos, el peso no está estable
        if not raw:
            return jsonify({"error": "Peso no estable todavía"}), 204

        # Normalizar el texto recibido
        texto = re.sub(r'\s+', ' ', raw.strip().lower())
        
        # Extraer valor numérico usando expresión regular
        match = re.search(r'(\d+(?:\.\d+)?)', texto)
        if not match:
            return jsonify({"error": f"No se pudo interpretar el peso: '{texto}'"}), 400

        valor = float(match.group(1))

        return jsonify({
            "peso_bruto": f"{valor:.1f}",
        })

    except serial.SerialException as e:
        return jsonify({"error": f"Error relacionado con el cable USB o la conexion al indicador de peso"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@main_bp.route('/puertos_com')
@login_required
def listar_puertos_com():
    """
    Retorna una lista JSON con los puertos seriales disponibles y su descripción.
    """
    puertos = list_ports.comports()
    lista_puertos = []
    for puerto in puertos:
        lista_puertos.append({
            "device": puerto.device,
            "descripcion": puerto.description
        })

    return jsonify({
        "puertos_com": lista_puertos
    })


@main_bp.route('/guardar_datos', methods=['POST'])
@login_required
def guardar_datos():
    # Convertir los datos del formulario en un dict
    datos = request.form.to_dict()

    #validacion
    campos_requeridos = ['fda', 'lote_basico', 'peso_bruto', 'peso_tara', 'sku_tina']
    for campo in campos_requeridos:
        if campo not in datos or not datos[campo].strip():
            flash(f'⚠️ El campo "{campo}" es obligatorio.', 'danger')
            return redirect(request.referrer)

    from datetime import datetime
    fecha_hora_guardado = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    fda = int(datos['fda'])
    fda_formateado = "L" + str(fda).zfill(3)
    lote_basico = datos['lote_basico'].upper()
    datos['fda'] = fda_formateado
    datos['lote_fda'] = lote_basico + fda_formateado
    datos['lote_sap'] = lote_basico + fda_formateado + "-" + datos['sku_tina']
    peso_bruto  = float(datos['peso_bruto'])
    # peso_bruto  = 900
    texto = datos['peso_tara'] # lo que imprime es 'Tara: 171 Kg'
    tara_extraida_de_texto = re.search(r'\d+', texto)
    tara = int(tara_extraida_de_texto.group()) if tara_extraida_de_texto else 0
    if tara == 0:
        tara = float(datos['nueva_tara'])
    peso_neto = peso_bruto - tara
    datos['tara'] = str(tara)
    datos['nombre_del_archivo'] = datos['lote_basico']
    datos['peso_neto'] = f"{peso_neto:.1f}"
    # datos['empleado'] = session.get('username') or ''

    datos['fecha_hora_guardado'] = fecha_hora_guardado
    # Instanciar servicios
    sqlite_service = SQLiteService()
    api_service = APIService(
        url='https://procesa.app/camaras_guardar_registro.php',
        api_key='z5fpYdhDtsuUBSrY6z6FypkjQPn0NiVz'
    )

    # Intentar guardar en la nube
    try:
        resultado = api_service.guardar(datos)
        id_procesa_app = resultado.get('id')
    except requests.exceptions.RequestException as e:
        log_error(f"⚠️ No se pudo enviar a la nube: {e}", archivo=__file__)
        id_procesa_app = ""

    # Asignar el id obtenido (o vacío)
    datos['id_procesa_app'] = id_procesa_app

    # Guardar siempre localmente
    try:
        id_local = sqlite_service.guardar(datos)
        # Si no se pudo guardar en la nube, agregar a la cola
        if not id_procesa_app:
            sqlite_service.agregar_a_cola("camaras_frigorifico", id_local, "INSERT")
    except Exception as e:
        mensaje = f"❌ Error local al guardar_datos: {str(e)}"
        log_error(mensaje, archivo=__file__)
        flash("❌ Ocurrió un error local al guardar los datos.", "danger")
    # Elimina los campos que no quieres enviar como query params
    datos.pop('csrf_token', None)
    datos.pop('select_puerto_com', None)
    datos.pop('certificado', None)
    datos.pop('fecha_hora_guardado', None)
    datos.pop('lote_sap', None)
    datos.pop('lote_fda', None)
    datos.pop('fda', None)
    datos.pop('tara', None)
    datos.pop('peso_neto', None)
    datos.pop('nueva_tara', None)
    datos.pop('hora_de_marbete', None)
    # datos.pop('tanque', None)
    datos.pop('observaciones', None)
    datos.pop('sku_tina', None)
    datos.pop('sku_talla', None)
    datos.pop('peso_tara', None)
    datos.pop('peso_bruto', None)
    datos.pop('hora_de_pesado', None)
    datos.pop('nombre_del_archivo', None)
    datos.pop('_external', None)
    # Redirige a la ruta `main.work` con los datos como query params
    return redirect(url_for('main.work', **datos)) # type: ignore



@main_bp.route('/descripcion_de_talla')
@login_required
def descripcion_de_talla():
    db = SQLiteService()
    try:
        mirespuesta = requests.get('https://procesa.app/endpoint_tinas.php',headers={'pass':'axRw0t31k8I8rDbLbItQY8ggz4x5iJr9'})
        if(mirespuesta.ok):
            data = mirespuesta.json()
            if isinstance(data, dict):
                data["desdeProcesa"] = 1
            else:
                data = {"datos": data, "desdeProcesa": 1}
        else:
            data = db.obtener_ultimos_13()
            data = {**data, "desdeProcesa": 0}
    except Exception as e:
        data = db.obtener_ultimos_13()
        data = {**data, "desdeProcesa": 0}
    return data

@main_bp.route('/buscar_barco')
@login_required
def buscar_barco():
    db = SQLiteService()
    inicial = request.args.get('inicial')
    string_inicial = ''
    if(inicial is not None):
        string_inicial = db.buscar_barco(inicial)
    return string_inicial

@main_bp.route('/buscar_peso_por_lote')
@login_required
def buscar_peso_por_lote():
    db = SQLiteService()
    lote_fda = request.args.get('lote_fda')
    total_peso = 0.0
    if lote_fda:
        total_peso = db.buscar_peso_por_lote(lote_fda)
    return str(total_peso)
    
@main_bp.route('/ultimos_registros')
@login_required
def ultimos_registros():
    db = SQLiteService()
    data = db.obtener_ultimos_13()
    data = {**data, "desdeProcesa": 0}

    # try:
    #     mirespuesta = requests.get('https://procesa.app/ultimos_5_recepcion_de_pescado.php',headers={'pass':'m8bdOmnm3uo8tt3Pfzi7iUAAKodiFOR3'})
    #     if(mirespuesta.ok):
    #         data = mirespuesta.json()
    #         if isinstance(data, dict):
    #             data["desdeProcesa"] = 1
    #         else:
    #             data = {"datos": data, "desdeProcesa": 1}
    #     else:
    #         data = db.obtener_ultimos_13()
    #         data = {**data, "desdeProcesa": 0}
    # except Exception as e:
    #     data = db.obtener_ultimos_13()
    #     data = {**data, "desdeProcesa": 0}
    return data

@main_bp.route('/migrar')
@login_required
def migraciones():
    if session.get('role') != "admin":
        abort(403, description="Acceso denegado")
    
    db = SQLiteService()
    resultado = db.migraciones()
    return resultado

@main_bp.route('/busqueda_talla_por_sku')
@login_required
def busqueda_talla_por_sku():
    db = SQLiteService()
    sku_talla = request.args.get('sku_talla')
    string_sku_talla = ''
    if(sku_talla is not None):
        string_sku_talla = db.buscar_talla_por_sku(sku_talla)
    return string_sku_talla

@main_bp.route('/descripcion_talla')
@login_required
def descripcion_talla():
    db = SQLiteService()
    sku_talla = request.args.get('sku_talla')
    string_sku_talla = ''
    if(sku_talla is not None):
        string_sku_talla = db.descripcion_talla(sku_talla)
    return string_sku_talla

@main_bp.route('/peso_tara')
@login_required
def peso_tara():
    db = SQLiteService()
    sku_tina = request.args.get('sku_tina')
    string_peso_tara = ''
    if(sku_tina is not None):
        string_peso_tara = db.obtener_peso_tara(sku_tina)
    # try:
    #     mirespuesta = requests.post("https://procesa.app/peso_tara.php", data={"consulta0": sku_tina})
    #     if(mirespuesta.ok):
    #         peso_tara = mirespuesta.json()
    #     else:
    #         peso_tara = db.obtener_ultimos_13()
    # except Exception as e:
    #     peso_tara = db.obtener_ultimos_13()
    return string_peso_tara

@main_bp.route('/eliminar_registro/<int:id>/<string:id_procesa_app>', methods=['GET'])
@login_required
def eliminar_registro(id, id_procesa_app):
    try:
        sqlite_service = SQLiteService()
        sqlite_service.marcar_como_borrado(id)

        if id_procesa_app != '-1':
            try:
                api_service = APIService(
                    url='https://procesa.app/eliminar_recepcion_pescado.php',
                    api_key='m8bdOmnm3uo8tt3Pfzi7iUAAKodiFOR3'
                )
                api_service.eliminar(id_procesa_app)

            except Exception as e:
                sqlite_service.agregar_a_cola(
                    tabla="camaras_frigorifico",
                    id_registro=id,
                    tipo_operacion="DELETE"
                )
                print("⚠️ No hay internet, DELETE pendiente agregado a la cola")

        return jsonify({"ok": True})

    except Exception as e:
        log_error(f"❌ Error en eliminar_registro: {e}", archivo=__file__)
        return jsonify({"error": "No se pudo eliminar"}), 500

@main_bp.route('/actualizar_campo', methods=['POST'])
def actualizar_campo():
    sqlite_service = SQLiteService()
    api_service = APIService(
        url='https://procesa.app/actualizar_campos_recepcion_barco.php',
        api_key='m8bdOmnm3uo8tt3Pfzi7iUAAKodiFOR3'
    )

    data = request.get_json()
    id_local = data.get('id')
    campo = data.get('campo')
    valor = data.get('valor')

    if not id_local or not campo or not valor:
        return jsonify({'success': False, 'message': 'ID o campo inválido'}), 400

    try:
        # 1 Actualizar local
        sqlite_service.actualizar_campo(id_local, campo, valor)
        # 2 actualiza el peso neto en caso de ser necesario
        if campo in ['peso_bruto', 'tara']:
            registro = sqlite_service.obtener_registro_por_id(id_local)
            peso_bruto = registro.get('peso_bruto') or 0
            tara = registro.get('tara') or 0
            peso_neto = float(peso_bruto) - float(tara)
            sqlite_service.actualizar_campo(id_local, 'peso_neto', peso_neto)
        # 3 Intentar actualizar en la nube
        id_procesa_app = sqlite_service.obtener_id_nube(id_local)
        if id_procesa_app:
            registro = sqlite_service.obtener_registro_por_id(id_local)
            try:
                api_service.actualizar(registro)
            except Exception as e:
                log_error(f"⚠️ No se pudo actualizar en la nube: {e}", archivo=__file__)
                # Si falla, agregar a la cola
                sqlite_service.agregar_a_cola("camaras_frigorifico", id_local, "UPDATE")
        else:
            # Si no tiene id_procesa_app aún, se guarda en la cola
            sqlite_service.agregar_a_cola("camaras_frigorifico", id_local, "INSERT")

        return jsonify({'success': True, 'message': 'Campo actualizado correctamente'})
    
    except Exception as e:
        log_error(f"❌ Error al actualizar campo: {e}", archivo=__file__)
        return jsonify({'success': False, 'message': str(e)}), 500