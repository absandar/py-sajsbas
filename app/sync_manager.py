import json, time, requests
from threading import Lock
from app.services.sqlite_service import SQLiteService
from app.utils.logger import log_error
from app.utils.sincronizador import SincronizadorSimple
from config import Config

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
        config = Config()
        sqlite_service = SQLiteService()
        sincronizador = SincronizadorSimple(
            sqlite_service,
            api_url="https://procesa.app/sincronizar_remisiones_v2.php",
            api_key="m8bdOmnm3uo8tt3Pfzi7iUAAKodiFOR3"
        )

        while True:
        # === LEER solo config LOCAL desde el archivo JSON ===
            try:
                with open(Config.LOCAL_CONFIGS, "r", encoding="utf-8") as f:
                    config = json.load(f)
            except FileNotFoundError:
                print("⚠ No se encontró configuraciones_varias.json. Se asume sync_enabled=True.")
                config = {"sync_enabled": True}
            except Exception as e:
                print(f"⚠ Error leyendo configuraciones_varias.json: {e}")
                config = {"sync_enabled": True}

            # === RESPETAR sync_enabled ===
            if not config.get("sync_enabled", True):
                print("⛔ Sync desactivada desde la configuración local")
                time.sleep(60 * 10)
                continue

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