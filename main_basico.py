from flask import Flask, render_template
from waitress import serve # Importa serve de waitress

# Crea una instancia de la aplicación Flask
app = Flask(__name__)

# Define una ruta para la página de inicio
@app.route('/')
def home():
    """
    Renderiza la plantilla 'index.html' para la página de inicio.
    """
    return render_template('index.html')

# Define otra ruta de ejemplo
@app.route('/about')
def about():
    """
    Renderiza la plantilla 'about.html' para la página "Acerca de".
    """
    return render_template('about.html')

# Punto de entrada para ejecutar la aplicación
if __name__ == '__main__':
    # Este bloque solo se ejecuta cuando main.py se corre directamente
    print("Iniciando la aplicación en modo de desarrollo con Flask built-in server...")
    app.run(debug=True, host='0.0.0.0', port=5000) # Se recomienda usar host='0.0.0.0' para que sea accesible desde otras máquinas en la red local