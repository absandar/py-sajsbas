from flask import render_template, redirect, url_for, flash, request, session
from app.auth import auth_bp
from app.forms import LoginForm
from app.models import User
from functools import wraps

# Decorador para requerir login
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            # flash('Necesitas iniciar sesión para acceder a esta página.', 'warning')
            return redirect(url_for('auth.login'))
        return f(*args, **kwargs)
    return decorated_function

# Decorador para requerir rol de administrador
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'role' not in session or session['role'] != 'admin':
            flash('No tienes permisos de administrador para acceder a esta página.', 'danger')
            return redirect(url_for('main.work')) # Redirige a la página de trabajo si no es admin
        return f(*args, **kwargs)
    return decorated_function

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    form = LoginForm()
    if form.validate_on_submit():
        user = User.get(form.username.data)
        if user and user.check_password(form.password.data):
            session['username'] = user.username
            session['role'] = user.role
            # flash(f'¡Bienvenido, {user.username}!', 'success')
            next_page = request.args.get('next') # Redirigir a la página previa si estaba intentando acceder
            return redirect(next_page or url_for('main.work'))
        else:
            flash('Usuario o contraseña incorrectos.', 'danger')
    return render_template('auth/login.html', form=form)

@auth_bp.route('/logout')
@login_required
def logout():
    session.pop('username', None)
    session.pop('role', None)
    # flash('Has cerrado sesión exitosamente.', 'info')
    return redirect(url_for('index')) # Redirige a la página de inicio