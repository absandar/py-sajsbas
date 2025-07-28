import requests
from .interfaces import AlmacenamientoBase

class APIService(AlmacenamientoBase):
    def __init__(self, url, api_key):
        self.url = url
        self.api_key = api_key

    def guardar(self, datos: dict):
        response = requests.post(self.url, json=datos, headers={
            'pass': f'{self.api_key}'
        })
        response.raise_for_status()
        return response.json().get("id")

    def eliminar(self, id: int):
        headers = {'pass': self.api_key}
        url_con_id = f"{self.url}?id={id}"
        response = requests.post(url_con_id, headers=headers)
        if response.status_code != 200:
            raise Exception("Error en API al borrar remotamente")
