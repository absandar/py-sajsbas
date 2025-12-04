import json, time, requests
from threading import Lock
from app.services.sqlite_service import SQLiteService
from app.utils.logger import log_error
from app.utils.sincronizador import SincronizadorSimple

class SyncManager:
    def __init__(self):
        self._lock = Lock()

    def respaldo_tablas(self):
        """Descarga catálogos de la nube (solo tinas, tallas y barcos)"""
        try:
            service = SQLiteService()
            # Catálogo de tina
            try:
                r = requests.get('https://procesa.app/endpoint_taras.php',
                                 headers={'pass': 'aEbHhguJNrw3EARMu9QVnT2lUZ2KQpde'})
                if r.ok:
                    service.guardar_catalogo_de_tina(r.json())
            except Exception as e:
                log_error(f"Error en catalogo_de_tina: {e}", archivo=__file__)

            # Catálogo de talla
            try:
                r = requests.get('https://procesa.app/endpoint_tinas.php',
                                 headers={'pass': 'axRw0t31k8I8rDbLbItQY8ggz4x5iJr9'})
                if r.ok:
                    service.guardar_catalogo_de_talla(r.json())
            except Exception as e:
                log_error(f"Error en catalogo_de_talla: {e}", archivo=__file__)

            # Catálogo de barcos
            try:
                r = requests.get('https://procesa.app/endpoint_barcos.php',
                                 headers={'pass': 'aEbHhguJNrw3EARMu9QVnT2lUZ2KQpde'})
                if r.ok:
                    service.guardar_catalogo_barcos(r.json())
            except Exception as e:
                log_error(f"Error en catalogo_barcos: {e}", archivo=__file__)

        except Exception as e:
            log_error(f"Error general en respaldo_tablas: {e}", archivo=__file__)

    def sincronizacion_periodica(self):
        sqlite_service = SQLiteService()
        sincronizador = SincronizadorSimple(
            sqlite_service,
            api_url="https://procesa.app/sincronizar_remisiones_v2.php",
            api_key="m8bdOmnm3uo8tt3Pfzi7iUAAKodiFOR3"
        )

        while True:
            if self._lock.locked():
                print("⏩ Sincronización previa en curso")
            else:
                with self._lock:
                    sincronizador.sincronizar()
            time.sleep(60 * 10)  # cada 10 minutos

    def sincronizar_manual(self):
        sqlite_service = SQLiteService()
        sincronizador = SincronizadorSimple(
            sqlite_service,
            api_url="https://procesa.app/sincronizar_remisiones_v2.php",
            api_key="m8bdOmnm3uo8tt3Pfzi7iUAAKodiFOR3"
        )
        with self._lock:
            resultado = sincronizador.sincronizar()
        return json.dumps(resultado)