from datetime import date
import re
import requests
import serial
import json
from serial.tools import list_ports
import time
from flask import Response, redirect, render_template, request, session, url_for, jsonify, flash
from app.auth.routes import login_required
from app.main import main_bp
from app.services.api_service import APIService
from app.services.sqlite_service import SQLiteService
from app.utils.logger import log_error
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
    peso_neto = peso_bruto - tara
    datos['tara'] = str(tara)
    datos['nombre_del_archivo'] = datos['lote_basico']
    datos['peso_neto'] = f"{peso_neto:.1f}"

    datos['fecha_hora_guardado'] = fecha_hora_guardado
    # Instanciar servicios
    sqlite_service = SQLiteService()
    api_service = APIService(
        url='https://procesa.app/camaras_guardar_registro.php',
        api_key='z5fpYdhDtsuUBSrY6z6FypkjQPn0NiVz'
    )
    errores = []
    try:
        # guardar en procesa.app que me devuelve el id
        id_procesa_app = api_service.guardar(datos)
        datos['id_procesa_app'] = id_procesa_app

        # guardar en sqlite
        sqlite_service.guardar(datos)
    except requests.exceptions.HTTPError as e:
        # Si el error viene con respuesta JSON del backend PHP
        try:
            error_json = e.response.json()
            mensaje = f"❌ Error HTTP desde API externa: {e} | Respuesta: {error_json}"
        except Exception:
            mensaje = f"❌ Error HTTP desde API externa: {e} | Respuesta: {e.response.text}"
        errores.append(mensaje)
        log_error(mensaje)
        flash("❌ Error remoto al guardar datos en procesa.app", "danger")

    except Exception as e:
        mensaje = f"❌ Error local al guardar_datos: {str(e)}"
        errores.append(mensaje)
        log_error(mensaje)
        flash("❌ Ocurrió un error local al guardar los datos.", "danger")

    #-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
    # # Armar respuesta
    # salida = ""
    # for clave, valor in datos.items():
    #     salida += f"{clave} => {valor}\n"

    # if errores:
    #     salida += "\n--- Errores ---\n" + "\n".join(errores)

    # return Response(salida, mimetype='text/plain')
    #-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|
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



@main_bp.route('/eliminar_registro/<int:id>')
@login_required
def eliminar_registro(id):
    try:
        # borrado local y remoto
        sqlite_service = SQLiteService()
        sqlite_service.marcar_como_borrado(id)

        api_service = APIService(
            url='https://procesa.app/eliminar_recepcion_pescado.php',
            api_key='m8bdOmnm3uo8tt3Pfzi7iUAAKodiFOR3'
        )
        api_service.eliminar(id)

        return jsonify({"ok": True})
    except Exception as e:
        log_error(f"❌ Error en eliminar_registro: {e}")
        return jsonify({"error": "No se pudo eliminar"}), 500