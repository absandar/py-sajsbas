import os
from dotenv import load_dotenv

load_dotenv()
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'una_clave_secreta_por_defecto_si_no_hay_env'
    DB_PATH = os.path.join(BASE_DIR, 'database.sqlite3')
    LOCAL_CONFIGS = os.path.join(BASE_DIR, 'configuraciones_varias.json')
    EXPORT_FOLDER = os.environ.get('EXPORT_FOLDER')