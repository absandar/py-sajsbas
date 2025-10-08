import os
import sys
from app import create_app
from waitress import serve
from dotenv import load_dotenv

load_dotenv()  # Carga las variables de entorno

app = create_app()

if __name__ == '__main__':
    # Lee el modo desde el primer argumento, si no se pasa usa 'development'
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else 'development'

    host = '127.0.0.1'
    port = 8087 if mode == 'development' else 8088 if mode == 'production' else 5000

    url = f"http://{host}:{port}"

    if mode == 'development':
        print(f"Iniciando la aplicación en modo de desarrollo con Flask built-in server en {url} ...")
        app.run(debug=True, host=host, port=port)
    elif mode == 'production':
        print(f"Iniciando la aplicación en modo de producción con Waitress en {url} ...")
        app.config['DEBUG'] = False
        serve(app, host=host, port=port)
    else:
        print(f"Modo desconocido: {mode}. Use 'development' o 'production'.")
        print(f"Por defecto, se inicia en modo de desarrollo en {url} ...")
        app.run(debug=True, host=host, port=port)
