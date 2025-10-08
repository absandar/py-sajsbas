import os
from datetime import datetime

LOG_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../logs.txt'))

def log_error(mensaje: str, archivo: str = 'No expecificado'):
    """
    Guarda un mensaje de error con marca de tiempo en logs.txt
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    nombre_archivo = os.path.basename(archivo) if archivo else ""

    info_archivo = f"[{nombre_archivo}]" if nombre_archivo else ""
    linea = f"[{timestamp}]{info_archivo} {mensaje}\n"

    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(linea)