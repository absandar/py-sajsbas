from werkzeug.security import generate_password_hash, check_password_hash

# Simulación de una base de datos de usuarios
users = {
    "55284": {
        "username": "tahiri",
        "password_hash": generate_password_hash("55284"),
        "role": "user"
    },
    "camaras": {
        "username": "camaras",
        "password_hash": generate_password_hash("camaras"),
        "role": "user"
    },
    "admin1": {
        "username": "admin1",
        "password_hash": generate_password_hash("adminpass"),
        "role": "admin"
    }
}

class User:
    def __init__(self, username, password_hash, role):
        self.username = username
        self.password_hash = password_hash
        self.role = role

    @staticmethod
    def get(username):
        user_data = users.get(username)
        if user_data:
            return User(user_data["username"], user_data["password_hash"], user_data["role"])
        return None

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    # Métodos requeridos por Flask-Login (aunque no lo usaremos directamente, es buena práctica)
    @property
    def is_authenticated(self):
        return True

    @property
    def is_active(self):
        return True

    @property
    def is_anonymous(self):
        return False

    def get_id(self):
        return self.username