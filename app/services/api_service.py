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
        # print(response.json())
        return response.json().get("id") or ""

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
        if response.status_code != 200:
            raise Exception("Error en API al borrar remotamente")
