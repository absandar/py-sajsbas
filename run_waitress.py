from waitress import serve
from main_basico import app # Importa la instancia 'app' de tu main.py

if __name__ == '__main__':
    HOST='0.0.0.0'
    print("Iniciando la aplicación con Waitress...")
    print(f"http://{HOST}:8080")
    # Puedes configurar el host y el puerto aquí
    # host='0.0.0.0' lo hace accesible desde otras máquinas en tu red local
    # port=8080 es un puerto común para producción, pero puedes usar 5000 si prefieres
    serve(app, host=HOST, port=8080)