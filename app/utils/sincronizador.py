import requests
from app.services.sqlite_service import SQLiteService
from app.services.api_service import APIService
class Sincronizador:
    def __init__(self, sqlite_service: SQLiteService, api_service: APIService, eliminar_api_service: APIService):
        self.sqlite_service = sqlite_service
        self.api_service = api_service
        self.eliminar_api_service = eliminar_api_service

    def sincronizar(self):
        pendientes = self.sqlite_service.obtener_pendientes_cola()
        if not pendientes:
            print("✅ No hay registros en la cola")
            return

        print(f"🔄 Sincronizando {len(pendientes)} registros...")

        for tarea in pendientes:
            try:
                registro = self.sqlite_service.obtener_registro(tarea['tabla'], tarea['id_registro'])
                
                # Marcar como procesado antes de enviar a la nube
                self.sqlite_service.marcar_como_procesado(tarea['id'])

                if tarea['tipo_operacion'] == "INSERT":
                    resultado = self.api_service.guardar(registro)
                    id_procesa_app = resultado.get('id') or resultado.get('actualizado')
                    if not id_procesa_app or not isinstance(id_procesa_app, int):
                        raise ValueError(f"ID de la nube inválido recibido: '{id_procesa_app}'")
                    self.sqlite_service.actualizar_id_nube(tarea['tabla'], tarea['id_registro'], int(id_procesa_app))
                elif tarea['tipo_operacion'] == "UPDATE":
                    self.api_service.actualizar(registro)
                elif tarea['tipo_operacion'] == "DELETE":
                    self.eliminar_api_service.eliminar(registro['id_procesa_app'])

                print(f"✅ {tarea['tipo_operacion']} procesado para {tarea['tabla']} id={tarea['id_registro']}")
            except requests.exceptions.HTTPError as e:
                # Respuesta HTTP inválida
                self.sqlite_service.desmarcar_como_procesado(tarea['id'])
                response = e.response
                print(f"❌ Error HTTP {response.status_code} en tarea {tarea['id']}")
                try:
                    print("Respuesta del backend:", response.json())
                except Exception:
                    print("Respuesta del backend:", response.text)

            except Exception as e:
                self.sqlite_service.desmarcar_como_procesado(tarea['id'])
                print(f"⚠️ Error al procesar tarea {tarea['id']}: {e}")
