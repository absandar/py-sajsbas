from datetime import date, datetime, timedelta
import os
import re
import tempfile
from zoneinfo import ZoneInfo
import imgkit
import requests
import serial
import json
import threading
from serial.tools import list_ports
import time
from flask import Response, redirect, render_template, request, send_file, session, url_for, jsonify, flash, abort
from app.auth.routes import login_required
from app.main import main_bp
from app.services.api_service import APIService
from app.services.excel_service import RemisionExcelBuilder
from app.services.sqlite_service import SQLiteService
from app.utils.logger import log_error
from app.utils.gestion_tinas import dividir_tina
from app.sync_manager import SyncManager
from config import Config


def buscar_puerto_por_numero_serie(serial_objetivo):
    """Devuelve el puerto COM asignado a un número de serie USB específico."""
    for port in list_ports.comports():
        if port.serial_number == serial_objetivo:
            return port.device
    return None


def _rango_semana_iso_dt(year: int, week: int):
    inicio = datetime.fromisocalendar(year, week, 1).replace(
        hour=0, minute=0, second=0, microsecond=0, tzinfo=ZoneInfo("America/Mexico_City")
    )
    fin_exclusivo = inicio + timedelta(days=7)  # [inicio, fin)
    return inicio, fin_exclusivo


def _etiquetas_rango(inicio_dt: datetime):
    ini = inicio_dt.strftime("%d %b")
    fin = (inicio_dt + timedelta(days=6)).strftime("%d %b")
    return ini, fin


@main_bp.route('/remisiones')
@login_required
def remisiones():
    """
    Página de trabajo para usuarios autenticados.
    """
    nomina = session.get('username')
    fecha_hoy = date.today().isoformat()
    params = request.args.to_dict()

    tanque = request.args.get("tanque")
    talla = request.args.get("talla")
    lote = request.args.get("lote")
    return render_template('main/remisiones.html',
                           tanque=tanque,
                           talla=talla,
                           lote=lote)


@main_bp.route('/guardar_remision', methods=['POST'])
@login_required
def guardar_remision():
    se_divide = False
    es_devolucion = False

    # === Detectar división y devolución ===
    dvd_nueva_carga = request.form.get("dvd_nueva_carga", "").strip()
    dvd_cantidad_solicitada = request.form.get("dvd_cantidad_solicitada", "").strip()
    dvd_tina_nueva = request.form.get("dvd_tina_nueva", "").strip()
    peso_bascula_division = request.form.get("peso_bascula_division", "").strip()
    peso_neto_division = int(request.form.get("peso_neto_division") or 0)

    if dvd_nueva_carga and dvd_cantidad_solicitada and dvd_tina_nueva and peso_bascula_division and peso_neto_division:
        se_divide = True

    btn_sensorial = request.form.get("btn_sensorial", "").strip()
    peso_tara = int(request.form.get("peso_tara_numero") or 0)
    peso_bascula_devolucion = request.form.get("peso_bascula_devolucion", "").strip()
    peso_neto_devolucion = request.form.get("peso_neto_devolucion", "").strip()
    tina_entrega = request.form.get("tina_entrega", "").strip()

    if tina_entrega and peso_bascula_devolucion and peso_neto_devolucion:
        es_devolucion = True

    # === Construir observaciones ===
    observaciones = ""
    if es_devolucion:
        observaciones = f"DEVOLUCION {tina_entrega}"
    elif se_divide:
        observaciones = f"SE DIVIDE {dvd_tina_nueva}"
    if btn_sensorial == 'on':
        is_sensorial = 1
    else:
        is_sensorial = 0

    # === Datos principales ===
    data = {
        # Nivel general
        "folio": request.form.get("folio", "").strip(),
        "cliente": request.form.get("cliente", "").strip(),
        "numero_sello": request.form.get("numero_sello", "").strip(),
        "placas_contenedor": request.form.get("placas_contenedor", "").strip(),
        "factura": request.form.get("factura", "").strip(),
        # Nivel cabecera
        "carga": request.form.get("carga", "").strip(),
        "cantidad_solicitada": request.form.get("cantidad_solicitada", "").strip(),
        # Nivel cuerpo
        "sku_tina": request.form.get("sku_tina", "").strip(),
        "sku_talla": request.form.get("sku_talla", "").strip(),
        "tara": peso_tara,
        "peso_neto": request.form.get("peso_neto", "").strip(),
        "merma": int(float(request.form.get("merma") or 0)),
        "lote": request.form.get("lote", "").strip(),
        "tanque": request.form.get("tanque", "").strip(),
        "peso_marbete": request.form.get("peso_marbete", "").strip(),
        "peso_bascula": int(request.form.get("peso_bascula") or 0),
        "peso_neto_devolucion": request.form.get("peso_neto_devolucion", "").strip(),
        "peso_bruto_devolucion": request.form.get("peso_bascula_devolucion", "").strip(),
        "observaciones": observaciones,
        "is_sensorial": is_sensorial,
    }

    # === Validación de campos obligatorios ===
    campos_obligatorios = [
        "carga", "cantidad_solicitada", "sku_tina", "sku_talla", "lote",
        "tanque", "peso_marbete", "peso_bascula"
    ]
    faltantes = [c for c in campos_obligatorios if not str(data.get(c, "")).strip()]
    if faltantes:
        flash(f"Faltan los siguientes campos: {', '.join(faltantes)}", "danger")
        return redirect(url_for('main.remisiones'))

    # === Validar campos numéricos ===
    try:
        int(data["carga"])
        int(data["cantidad_solicitada"])
        float(data["peso_marbete"])
        float(data["peso_bascula"])
        if data["tara"]:
            float(data["tara"])
        if data["peso_neto"]:
            float(data["peso_neto"])
        if data["merma"]:
            float(data["merma"])
    except (ValueError, TypeError):
        flash("Error: Algunos campos numéricos contienen valores inválidos", "danger")
        return redirect(url_for('main.remisiones'))

    # === Guardar datos ===
    try:
        sqliteService = SQLiteService()
        sqliteService.guardar_remision(data)

        # === Caso especial: se divide ===
        if se_divide:
            fila_1_bruto = data["peso_bascula"] - peso_neto_division
            fila_2_bruto = (data["peso_bascula"] - fila_1_bruto) + peso_tara
            fila_1_neto = fila_1_bruto - peso_tara
            fila_2_neto = fila_2_bruto - peso_tara
            fila_1_merma = 0
            fila_2_merma = data["merma"]
            fila_1_marbete = fila_1_neto + fila_1_merma
            fila_2_marbete = fila_2_neto + fila_2_merma

            data_dividida = data.copy()
            data_dividida.update({
                "carga": dvd_nueva_carga,
                "cantidad_solicitada": dvd_cantidad_solicitada,
                "peso_bascula": fila_2_bruto,
                "peso_neto": fila_2_neto,
                "merma": fila_2_merma,
                "peso_marbete": fila_2_marbete,
                "observaciones": f"SE DIVIDE {dvd_tina_nueva}",
                "is_sensorial": is_sensorial,
            })
            sqliteService.guardar_remision(data_dividida)

    except Exception as e:
        flash(f"Ocurrió un error al guardar los datos: {str(e)}", "danger")

    # === Redirigir ===
    return redirect(url_for('main.remisiones',
                            tanque=data["tanque"],
                            talla=data["sku_talla"],
                            lote=data["lote"]))
    # return redirect(url_for('main.remisiones'))

@main_bp.route('/sincronizador')
@login_required
def sincronizacion():
    return render_template("main/sincronizador.html")

@main_bp.route('/sincronizacion_manual')
@login_required
def sincronizacion_manual():
    sync = SyncManager()
    data = sync.sincronizar_manual()
    return data

@main_bp.route('/cargas_del_dia')
@login_required
def cargas_del_dia():
    db = SQLiteService()
    data = db.cargas_del_dia()
    return data

@main_bp.route('/obtener_retallados')
@login_required
def obtener_retallados():
    db = SQLiteService()
    id = request.args.get('id') or ""
    data = db.obtener_retallados(id)
    return data

@main_bp.route('/remisiones_del_dia_por_carga')
@login_required
def remisiones_del_dia_por_carga():
    db = SQLiteService()
    carga = request.args.get('carga')
    cantidad_solicitada = request.args.get('cantidad_solicitada')
    data = db.remisiones_del_dia_por_carga(carga, cantidad_solicitada)
    return data


@main_bp.route('/todas_las_remisiones')
@login_required
def todas_las_remisiones():
    now_mx = datetime.now(ZoneInfo("America/Mexico_City"))
    year, week, _ = now_mx.isocalendar()

    # Construir lista de semanas (1..53) con rango
    semanas = []
    for w in range(1, 54):
        try:
            ini, fin = _rango_semana_iso_dt(year, w)
            semanas.append({
                "num": w,
                "ini": ini.strftime("%d %b"),
                "fin": fin.strftime("%d %b"),
            })
        except ValueError:
            # Algunas semanas (ej. 53 en años que no tienen) pueden fallar
            continue

    return render_template("main/todas_las_remisiones_wrapper.html",
                           year=year, week=week, semanas=semanas)

@main_bp.route('/todas_las_remisiones_inner')
@login_required
def todas_las_remisiones_inner():
    year = request.args.get("year", type=int)
    week = request.args.get("week", type=int)

    # Defaults a semana actual si no vienen params (útil para etiquetas)
    now_mx = datetime.now(ZoneInfo("America/Mexico_City"))
    year_actual, week_actual, _ = now_mx.isocalendar()

    db = SQLiteService()

    if year and week:
        inicio_dt, fin_dt = _rango_semana_iso_dt(year, week)
        inicio_str = inicio_dt.strftime("%Y-%m-%d %H:%M:%S")
        fin_str = fin_dt.strftime("%Y-%m-%d %H:%M:%S")
        data = db.remisiones_por_rango(inicio_str, fin_str)
        ini_lbl, fin_lbl = _etiquetas_rango(inicio_dt)
        year_out, week_out = year, week
    else:
        data = db.todas_las_remisiones()
        # Etiquetas para la semana actual
        inicio_dt, _ = _rango_semana_iso_dt(year_actual, week_actual)
        ini_lbl, fin_lbl = _etiquetas_rango(inicio_dt)
        year_out, week_out = year_actual, week_actual

    return render_template(
        "main/todas_las_remisiones_inner.html",
        remisiones=data,
        year=year_out,
        week=week_out,
        rango_ini=ini_lbl,
        rango_fin=fin_lbl
    )


@main_bp.route('/remisiones_img')
@login_required
def remisiones_img():
    # Lee filtros
    year = request.args.get("year", type=int)
    week = request.args.get("week", type=int)

    db = SQLiteService()

    if year and week:
        inicio_dt, fin_dt = _rango_semana_iso_dt(year, week)  # datetimes
        inicio_str = inicio_dt.strftime("%Y-%m-%d %H:%M:%S")
        fin_str    = fin_dt.strftime("%Y-%m-%d %H:%M:%S")
        data = db.remisiones_por_rango(inicio_str, fin_str)

        # Etiquetas para el encabezado dentro del HTML que se volverá imagen
        rango_ini, rango_fin = _etiquetas_rango(inicio_dt)
        ctx = dict(
            remisiones=data,
            year=year,
            week=week,
            rango_ini=rango_ini,
            rango_fin=rango_fin
        )
    else:
        data = db.todas_las_remisiones()
        # Usa semana actual para los labels
        now_mx = datetime.now(ZoneInfo("America/Mexico_City"))
        year_act, week_act, _ = now_mx.isocalendar()
        inicio_dt, _ = _rango_semana_iso_dt(year_act, week_act)
        rango_ini, rango_fin = _etiquetas_rango(inicio_dt)
        ctx = dict(
            remisiones=data,
            year_act=year_act,
            week_act=week_act,
            rango_ini=rango_ini,
            rango_fin=rango_fin
        )

    # Renderiza el mismo inner con el mismo contexto que verías en pantalla
    html = render_template("main/todas_las_remisiones_inner.html", **ctx)

    # Generar imagen temporal
    output_path = os.path.join(tempfile.gettempdir(), "remisiones.png")
    imgkit.from_string(html, output_path)

    return send_file(output_path, mimetype='image/png')

@main_bp.route('/')
@login_required
def work():
    """
    Página de trabajo para usuarios autenticados.
    """
    nomina = session.get('username')
    fecha_hoy = date.today().isoformat()
    params = request.args.to_dict()
    return render_template('main/work.html', fecha_hoy=fecha_hoy, params=params, nomina=nomina)


@main_bp.route('/reportes')
@login_required  # Esta ruta requiere que el usuario esté logueado
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
@login_required  # Esta ruta requiere que el usuario esté logueado
def toda_la_data_de_local():
    sqliteService = SQLiteService()
    data = sqliteService.obtener_reportes()
    return jsonify(data)


@main_bp.route('/manual')
@login_required  # Esta ruta requiere que el usuario esté logueado
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

    # validacion
    campos_requeridos = ['fda', 'lote_basico',
                         'peso_bruto', 'peso_tara', 'sku_tina']
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
    peso_bruto = float(datos['peso_bruto'])
    # --- Tara ---
    tara = 0.0

    # 1) Prioridad: nueva_tara
    if 'nueva_tara' in datos and str(datos['nueva_tara']).strip():
        try:
            tara = float(str(datos['nueva_tara']).replace(',', '.'))
        except ValueError:
            tara = 0.0

    # 2) Si no hay nueva_tara válida, intentar con el texto (ej. "Tara: 171 Kg")
    if tara <= 0:
        texto = datos.get('peso_tara', '')
        match = re.search(r'(\d+(?:[.,]\d+)?)', texto)
        if match:
            tara = float(match.group(1).replace(',', '.'))

    peso_neto = peso_bruto - tara

    datos['tara'] = f"{tara:.1f}"
    datos['nombre_del_archivo'] = datos['lote_basico']
    datos['peso_neto'] = f"{peso_neto:.1f}"
    # datos['empleado'] = session.get('username') or ''

    datos['fecha_hora_guardado'] = fecha_hora_guardado
    # Instanciar servicios
    sqlite_service = SQLiteService()
    id_procesa_app = ""

    # Asignar el id obtenido (o vacío)
    datos['id_procesa_app'] = id_procesa_app

    # Guardar siempre localmente
    try:
        sqlite_service.guardar(datos)
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
    return redirect(url_for('main.work', **datos))  # type: ignore


@main_bp.route('/descripcion_de_talla')
@login_required
def descripcion_de_talla():
    db = SQLiteService()
    try:
        mirespuesta = requests.get('https://procesa.app/endpoint_tinas.php', headers={
                                   'pass': 'axRw0t31k8I8rDbLbItQY8ggz4x5iJr9'})
        if (mirespuesta.ok):
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
    if (inicial is not None):
        string_inicial = db.buscar_barco(inicial)
    return string_inicial

@main_bp.route('/descargar_excel')
@login_required
def descargar_excel():
    builder = RemisionExcelBuilder()
    if builder.cargas_de_dia != "{}":
        today_str = datetime.now(ZoneInfo("America/Mexico_City")).strftime("%Y-%m-%d %H:%M:%S")
        nombre_archivo = "remisiones_" + today_str + ".xlsx"
        siguiente_fila = builder.tabla_principal()
        siguiente_fila = builder.retallado(siguiente_fila) # type: ignore
        siguiente_fila = builder.totales(siguiente_fila)
        ruta_completa = builder.guardar(nombre_archivo)

        return send_file(ruta_completa, as_attachment=True, download_name=nombre_archivo)
    else:
        return '', 204

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

@main_bp.route('/busqueda_talla_por_sku')
@login_required
def busqueda_talla_por_sku():
    db = SQLiteService()
    sku_talla = request.args.get('sku_talla')
    string_sku_talla = ''
    if (sku_talla is not None):
        string_sku_talla = db.buscar_talla_por_sku(sku_talla)
    return string_sku_talla


@main_bp.route('/descripcion_talla')
@login_required
def descripcion_talla():
    db = SQLiteService()
    sku_talla = request.args.get('sku_talla')
    string_sku_talla = ''
    if (sku_talla is not None):
        string_sku_talla = db.descripcion_talla(sku_talla)
    return string_sku_talla


@main_bp.route('/peso_tara')
@login_required
def peso_tara():
    db = SQLiteService()
    sku_tina = request.args.get('sku_tina')
    string_peso_tara = ''
    if (sku_tina is not None):
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


@main_bp.route('/eliminar_registro/<id>', methods=['GET'])
@login_required
def eliminar_registro(id):
    try:
        sqlite_service = SQLiteService()
        sqlite_service.marcar_como_borrado(id)
        return jsonify({"ok": True})

    except Exception as e:
        log_error(f"❌ Error en eliminar_registro: {e}", archivo=__file__)
        return jsonify({"error": "No se pudo eliminar"}), 500


@main_bp.route('/actualizar_campo', methods=['POST'])
def actualizar_campo():
    sqlite_service = SQLiteService()
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

        return jsonify({'success': True, 'message': 'Campo actualizado correctamente'})

    except Exception as e:
        log_error(f"❌ Error al actualizar campo: {e}", archivo=__file__)
        return jsonify({'success': False, 'message': str(e)}), 500


@main_bp.route('/actualizar_campo_remision', methods=['POST'])
def actualizar_campo_remision():
    sqlite_service = SQLiteService()
    data = request.get_json() or {}

    id_local = data.get('id')
    tabla = data.get('tabla')
    campo = data.get('campo')
    valor = data.get('valor')
    id_remision_general = data.get('id_remision_general')

    # Validar id y columnas
    if not tabla or not campo:
        return jsonify({'success': False, 'message': 'Datos inválidos'}), 400

    if id_local is None or str(id_local).strip().lower() in ('undefined', 'null'):
        return jsonify({'success': False, 'message': 'ID inválido'}), 400

    try:
        # Pasar el id_remision_general a la función y obtener el ID resultante
        nuevo_id = sqlite_service.actualizar_campo_remision(tabla, id_local, campo, valor, id_remision_general)
        
        response_data = {'success': True, 'message': 'Campo actualizado correctamente'}
        
        # Si se generó un nuevo ID (para retallados), devolverlo
        if tabla == "retallados" and nuevo_id != id_local:
            response_data['nuevo_id'] = nuevo_id
            
        return jsonify(response_data)
    except Exception as e:
        log_error(f"❌ Error en actualizar_campo_remision: {e}", archivo=__file__)
        return jsonify({'success': False, 'message': str(e)}), 500

@main_bp.route('/devolucion')
@login_required
def devolucion():
    cantidad_solicitada = request.args.get('cantidad_solicitada', type=int)
    pesos_netos_str = request.args.get('pesos_netos', '')
    pesos_netos = [int(x) for x in pesos_netos_str.split(',') if x.strip().isdigit()]
    data = dividir_tina(cantidad_solicitada, pesos_netos)
    return jsonify(data)
