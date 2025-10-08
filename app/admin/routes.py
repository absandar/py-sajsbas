import json
from config import Config
from flask import render_template, session, request, flash, redirect, url_for
from app.admin import admin_bp
from app.auth.routes import login_required, admin_required

@admin_bp.route('/dashboard', methods=['GET', 'POST'])
@login_required # Esta ruta requiere que el usuario esté logueado
# @admin_required # Y que tenga el rol de admin
def dashboard():
    """
    Panel administrativo para usuarios con rol de admin.
    """
    username = session.get('username')
    with open(Config.LOCAL_CONFIGS, 'r') as f:
        data = json.load(f)
        mapa_actual = data.get("mapa_basculas", {})

    series_disponibles = list(set(mapa_actual.values()))  # elimina duplicados
    asignaciones = {v: k for k, v in mapa_actual.items()}  # para mostrar cuál tiene qué número

    if request.method == 'POST':
        nueva_asignacion = {}
        for serie in series_disponibles:
            numero = request.form.get(serie)
            if numero:
                nueva_asignacion[numero] = serie

        # Guardar
        data["mapa_basculas"] = nueva_asignacion
        with open(Config.LOCAL_CONFIGS, 'w') as f:
            json.dump(data, f, indent=2)

        flash("Asignación de básculas actualizada.", "success")
        return redirect(url_for('admin.dashboard'))

    return render_template('admin/dashboard.html',
                           username=username,
                           series=series_disponibles,
                           asignaciones=asignaciones)