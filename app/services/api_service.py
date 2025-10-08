import requests
import json
from app.utils.logger import log_error

class APIService():
    def __init__(self, url, api_key):
        self.url = url
        self.api_key = api_key

    def guardar(self, datos: dict):
        response = requests.post(self.url, json=datos, headers={
            'pass': f'{self.api_key}'
        })
        response.raise_for_status()
        print(response.json())
        return response.json()

    def actualizar(self, datos: dict):
        if not datos.get("id_procesa_app"):
            raise ValueError("No se puede actualizar: falta id_procesa_app")

        response = requests.post(
            f"{self.url}",
            json=datos,
            headers={'pass': self.api_key}
        )
        response.raise_for_status()
        return response.json()

    def eliminar(self, id: int):
        headers = {'pass': self.api_key}
        url_con_id = f"{self.url}?id={id}"
        response = requests.post(url_con_id, headers=headers)

        try:
            response.raise_for_status()  # lanza HTTPError si el status != 200
        except requests.exceptions.HTTPError as e:
            raise Exception(f"Error HTTP {response.status_code} en API al borrar: {response.text}") from e

        try:
            data = response.json()
        except ValueError:
            raise Exception(f"Respuesta inválida de la API al borrar (no es JSON): {response.text}")
        return data
