import os
from datetime import datetime

LOG_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../logs.txt'))

def log_error(mensaje: str):
    """
    Guarda un mensaje de error con marca de tiempo en logs.txt
    """
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    linea = f"[{timestamp}] {mensaje}\n"
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(linea)
