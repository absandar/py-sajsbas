from flask import render_template, session
from app.admin import admin_bp
from app.auth.routes import login_required, admin_required # Importa los decoradores

@admin_bp.route('/dashboard')
@login_required # Esta ruta requiere que el usuario esté logueado
@admin_required # Y que tenga el rol de admin
def dashboard():
    """
    Panel administrativo para usuarios con rol de admin.
    """
    username = session.get('username')
    return render_template('admin/dashboard.html', username=username)