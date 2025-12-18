import json
from config import Config
from flask import render_template, session, request, flash, redirect, url_for
from app.admin import admin_bp
from app.auth.routes import login_required, admin_required
from app.services.sqlite_service import SQLiteService

@admin_bp.route('/dashboard', methods=['GET', 'POST'])
@login_required
def dashboard():
    sqlite_service = SQLiteService()
    sqlite_service.crear_todas_las_tablas()

    username = session.get('username')
    with open(Config.LOCAL_CONFIGS, 'r') as f:
        data = json.load(f)

    if "sync_enabled" not in data:
        data["sync_enabled"] = True

    mapa_actual = data.get("mapa_basculas", {})
    export_folder_actual = data.get("EXPORT_FOLDER", "")
    sync_enabled_actual = data.get("sync_enabled", True)

    series_disponibles = list(set(mapa_actual.values()))
    asignaciones = {v: k for k, v in mapa_actual.items()}

    if request.method == 'POST':
        nueva_asignacion = {}
        for serie in series_disponibles:
            numero = request.form.get(serie)
            if numero:
                nueva_asignacion[numero] = serie
        data["mapa_basculas"] = nueva_asignacion
        nueva_ruta = request.form.get("export_folder", "").strip()
        if nueva_ruta:
            data["EXPORT_FOLDER"] = nueva_ruta
        sync_value = request.form.get("sync_enabled", "off")
        data["sync_enabled"] = True if sync_value == "on" else False
        with open(Config.LOCAL_CONFIGS, 'w') as f:
            json.dump(data, f, indent=2)

        flash("Configuraciones guardadas correctamente.", "success")
        return redirect(url_for('admin.dashboard'))

    return render_template(
        'admin/dashboard.html',
        username=username,
        series=series_disponibles,
        asignaciones=asignaciones,
        export_folder=export_folder_actual,
        sync_enabled=sync_enabled_actual
    )
