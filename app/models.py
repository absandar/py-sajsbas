from werkzeug.security import generate_password_hash, check_password_hash

# Simulación de una base de datos de usuarios
users = {
    "54859": {
        "username": "54859",
        "password_hash": generate_password_hash("54859"),
        "role": "user"
    },
    "55284": {
        "username": "55284",
        "password_hash": generate_password_hash("55284"),
        "role": "user"
    },
    "56455": {
        "username": "56455",
        "password_hash": 'scrypt:32768:8:1$50kJh3q7oWMnix6x$2b4f2902db7c6ffe8ac87ff293da1b9e8b08422c260e38d7568280f890f926d773f520931e818b3cd72f5b84f569ab479ec1ce5a78bffab2b1d7d33158273c99',
        "role": "admin"
    },
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