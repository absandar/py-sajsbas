import sqlite3
import requests
import json
from datetime import datetime
from app.utils.logger import log_error


class SincronizadorSimple:
    def __init__(self, sqlite_service, api_url, api_key):
        self.sqlite_service = sqlite_service
        self.api_url = api_url
        self.api_key = api_key

    def _hoy(self):
        return datetime.now().strftime("%Y-%m-%d")

    def _obtener_tabla(self, tabla, campo_fecha):
        """Devuelve todos los registros de hoy para la tabla dada"""
        conn = sqlite3.connect(self.sqlite_service.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            f"SELECT * FROM {tabla} WHERE DATE({campo_fecha}) = ?", (self._hoy(),))
        rows = [dict(row) for row in cur.fetchall()]
        conn.close()
        return rows


    def sincronizar(self):
        try:
            data = {
                "camaras_frigorifico": self._obtener_tabla("camaras_frigorifico", "fecha_hora_guardado"),
                "remisiones_cabecera": self._obtener_tabla("remisiones_cabecera", "fecha_creacion"),
                "remisiones_cuerpo": self.sqlite_service.obtener_remisiones_cuerpo_hoy()
            }

            if not any(data.values()):
                return {
                    "status": "ok",
                    "mensaje": f"No hay registros para sincronizar hoy {self._hoy()}",
                    "procesados": {k: 0 for k in data.keys()}
                }

            resp = requests.post(
                self.api_url,
                headers={"pass": self.api_key, "Content-Type": "application/json"},
                data=json.dumps(data)
            )

            if resp.ok:
                try:
                    backend_data = resp.json()
                except Exception:
                    backend_data = resp.text  # si no es JSON válido, regresamos el texto plano

                return {
                    "status": "ok",
                    "mensaje": "Sincronización exitosa",
                    "respuesta_backend": backend_data
                }
            else:
                return {
                    "status": "error",
                    "mensaje": f"Error HTTP {resp.status_code}",
                    "respuesta_backend": resp.text
                }

        except Exception as e:
            log_error(f"Error en sincronización: {e}", archivo=__file__)
            return {
                "status": "error",
                "mensaje": f"Excepción en sincronización: {str(e)}"
            }