from datetime import date
import re
import requests
import serial
import serial.tools.list_ports
import time
from flask import Response, redirect, render_template, request, session, url_for, jsonify, flash
from app.auth.routes import login_required
from app.main import main_bp
from app.services.api_service import APIService
from app.services.sqlite_service import SQLiteService
from app.utils.logger import log_error

@main_bp.route('/')
@login_required # Esta ruta requiere que el usuario esté logueado
def work():
    """
    Página de trabajo para usuarios autenticados.
    """
    username = session.get('username')
    fecha_hoy = date.today().isoformat()
    params = request.args.to_dict()
    return render_template('main/work.html', fecha_hoy=fecha_hoy, params=params, username = username)

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
    puerto = request.args.get('puerto', '').strip()

    if not puerto:
        return jsonify({"error": "Parámetro 'puerto' es requerido"}), 400

    try:
        ser = serial.Serial(puerto, baudrate=9600, timeout=1)
        ser.write(b'P\r\n')
        time.sleep(0.1)
        raw = ser.readline().decode('utf-8').strip()
        ser.close()

        if not raw:
            return jsonify({"error": "Peso no estable todavía"}), 204

        texto = re.sub(r'\s+', ' ', raw.strip()).lower()

        if texto.endswith('kg'):
            valor = texto.replace('kg', '').strip()
            try:
                peso = float(valor)
                return jsonify({
                    "peso_bruto": f"{peso:.1f}",
                    "unidad": "kg"
                })
            except ValueError:
                return jsonify({"error": f"Peso inválido: {valor}"}), 400
        else:
            return jsonify({
                "peso_bruto": texto,
                "unidad": "desconocido"
            })

    except serial.SerialException as e:
        return jsonify({"error": f"Error en el puerto serial: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@main_bp.route('/puertos_com')
@login_required
def listar_puertos_com():
    """
    Retorna una lista JSON con los puertos seriales disponibles y su descripción.
    """
    puertos = serial.tools.list_ports.comports()
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
    datos.pop('tanque', None)
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