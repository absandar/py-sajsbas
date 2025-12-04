import os
from threading import Thread 
from flask import Flask, redirect, url_for
from app.sync_manager import SyncManager
from config import Config
from flask_wtf.csrf import CSRFProtect
from app.auth.routes import login_required

# Importar Blueprints (se definirán después)
from app.auth import auth_bp
from app.main import main_bp
from app.admin import admin_bp

csrf = CSRFProtect()

def create_app():
    # Carpeta base donde está este archivo (__init__.py)
    base_dir = os.path.abspath(os.path.dirname(__file__))  # .../entrada_de_pescado_procesa/app
    # Pasa la ruta completa de la carpeta de plantillas a Flask
    app = Flask(__name__)
    app.config.from_object(Config)
    # ❌ Desactivar CSRF globalmente
    app.config['WTF_CSRF_ENABLED'] = False  
    # Inicializar extensiones
    csrf.init_app(app)

    # Registrar Blueprints
    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(main_bp, url_prefix='/') # Prefijo '/' para la página principal
    app.register_blueprint(admin_bp, url_prefix='/admin')

    # Ruta para la página de inicio (sin blueprint específico si quieres)
    @app.route('/')
    @login_required 
    def index():
        return redirect(url_for('main.work'))

    def sync():
        syncManager = SyncManager()
        syncManager.respaldo_tablas()
        syncManager.sincronizacion_periodica()

    # Iniciar hilo de sincronización
    Thread(target=sync, daemon=True).start()
    return app