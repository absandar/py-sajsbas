from app import create_app

app = create_app()

# Esto es el punto de entrada para servidores WSGI como Waitress, Gunicorn, etc.
# No necesitas un if __name__ == '__main__': aquí.