import json
import time
import requests
from threading import Lock
from app.services.api_service import APIService
from app.services.sqlite_service import SQLiteService
from app.utils.logger import log_error
from app.utils.sincronizador import Sincronizador


class SyncManager:
    def __init__(self):
        self._lock = Lock()
    def respaldo_tablas(self):
        """Hace respaldo local de todas las tablas necesarias."""
        try:
            service = SQLiteService()
            service._asegurar_tabla_cola_sincronizacion()

            # Catalogo de tina
            try:
                myresponse = requests.get('https://procesa.app/endpoint_taras.php',headers={'pass': 'aEbHhguJNrw3EARMu9QVnT2lUZ2KQpde'})
                if myresponse.ok:
                    jData = json.loads(myresponse.content)
                    service.guardar_catalogo_de_tina(jData)
            except Exception as e:
                log_error(f"Error en respaldo de catalogo_de_tina: {e}", archivo=__file__)

            # Catalogo de talla
            try:
                myresponse = requests.get('https://procesa.app/endpoint_tinas.php',headers={'pass': 'axRw0t31k8I8rDbLbItQY8ggz4x5iJr9'})
                if myresponse.ok:
                    jData = json.loads(myresponse.content)
                    service.guardar_catalogo_de_talla(jData)
            except Exception as e:
                log_error(f"Error en respaldo de catalogo_de_talla: {e}", archivo=__file__)

            # Recepcion barco
            try:
                myresponse = requests.get('https://procesa.app/endpoint_ultimos_datos_barco.php',headers={'pass': 'FFIwwww25oMq7K7w2TCLcH1eDYGrl4m0'})
                if myresponse.ok:
                    jData = json.loads(myresponse.content)
                    service.fusionar_con_local(jData)
            except Exception as e:
                log_error(f"Error en respaldo de recepcion_barco: {e}", archivo=__file__)
            # Catalogo de barco
            try:
                myresponse = requests.get('https://procesa.app/endpoint_barcos.php',headers={'pass': 'aEbHhguJNrw3EARMu9QVnT2lUZ2KQpde'})
                if myresponse.ok:
                    jData = json.loads(myresponse.content)
                    service.guardar_catalogo_barcos(jData)
            except Exception as e:
                log_error(f"Error en respaldo de recepcion_barco: {e}", archivo=__file__)

        except Exception as e:
            log_error(f"Error general en respaldo_tablas: {e}", archivo=__file__)

    def sincronizacion_periodica(self):
        sqlite_service = SQLiteService()
        api_service = APIService(
            url='https://procesa.app/actualizar_campos_recepcion_barco.php',
            api_key='m8bdOmnm3uo8tt3Pfzi7iUAAKodiFOR3'
        )
        eliminar_api_service = APIService(
            url='https://procesa.app/eliminar_recepcion_pescado.php',
            api_key='m8bdOmnm3uo8tt3Pfzi7iUAAKodiFOR3'
        )
        sincronizador = Sincronizador(sqlite_service, api_service, eliminar_api_service)
        while True:
            if self._lock.locked():
                print("⏩ Sincronización previa aún en curso, se salta este ciclo")
            else:
                with self._lock:
                    try:
                        sincronizador.sincronizar()
                    except Exception as e:
                        print(f"⚠️ Error en sincronización automática: {e}")
            time.sleep(30)