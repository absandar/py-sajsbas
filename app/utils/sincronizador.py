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
            # incluir las tablas nuevas que existen en la app local
            def safe_obtener(tabla, campo):
                try:
                    return self._obtener_tabla(tabla, campo)
                except Exception:
                    return []
            
            data = {
                "camaras_frigorifico": safe_obtener("camaras_frigorifico", "fecha_hora_guardado"),
                "remisiones_general": safe_obtener("remisiones_general", "fecha_creacion"),
                "remisiones_cabecera": safe_obtener("remisiones_cabecera", "fecha_creacion"),
                "remisiones_cuerpo": self.sqlite_service.obtener_remisiones_cuerpo_hoy(),
                "remisiones_retallados": safe_obtener("remisiones_retallados", "fecha_creacion")
            }

            if not any(data.values()):
                return {
                    "status": "ok",
                    "mensaje": f"No hay registros para sincronizar hoy {self._hoy()}",
                    "procesados": {k: 0 for k in data.keys()}
                }

            # enviar payload
            resp = requests.post(
                self.api_url,
                headers={"pass": self.api_key, "Content-Type": "application/json"},
                data=json.dumps(data),
                timeout=30
            )

            if not resp.ok:
                log_error(f"Sincronización HTTP {resp.status_code}: {resp.text}", archivo=__file__)
                log_error(f"Payload enviado: {json.dumps(data)[:2000]}", archivo=__file__)
                return {
                    "status": "error",
                    "mensaje": f"Error HTTP {resp.status_code}",
                    "respuesta_backend": resp.text
                }

            try:
                backend_data = resp.json()
            except Exception:
                backend_data = resp.text  # si no es JSON válido, regresamos el texto plano

            return {
                "status": "ok",
                "mensaje": "Sincronización exitosa",
                "respuesta_backend": backend_data
            }
        except Exception as e:
            log_error(f"Error en sincronización: {e}", archivo=__file__)
            return {
                "status": "error",
                "mensaje": f"Excepción en sincronización: {str(e)}"
            }