from app.services.sqlite_service import SQLiteService
from app.services.api_service import APIService
class Sincronizador:
    def __init__(self, sqlite_service: SQLiteService, api_service: APIService):
        self.sqlite_service = sqlite_service
        self.api_service = api_service

    def sincronizar(self):
        pendientes = self.sqlite_service.obtener_pendientes_cola()
        if not pendientes:
            print("✅ No hay registros en la cola")
            return

        print(f"🔄 Sincronizando {len(pendientes)} registros...")

        for tarea in pendientes:
            try:
                # Recuperar el registro de la tabla correspondiente
                registro = self.sqlite_service.obtener_registro(
                    tarea['tabla'], tarea['id_registro']
                )

                # Mandar a la nube según tipo de operación
                if tarea['tipo_operacion'] == "INSERT":
                    id_procesa_app = self.api_service.guardar(registro)
                    if not id_procesa_app or not id_procesa_app.isdigit():
                        raise ValueError(f"ID de la nube inválido recibido: '{id_procesa_app}'")
                    id_procesa_app = int(id_procesa_app)
                    self.sqlite_service.actualizar_id_nube(
                        tarea['tabla'], tarea['id_registro'], id_procesa_app
                    )
                elif tarea['tipo_operacion'] == "UPDATE":
                    self.api_service.actualizar(registro)
                elif tarea['tipo_operacion'] == "DELETE":
                    self.api_service.eliminar(registro['id_procesa_app'])

                # Marcar como procesado en la cola
                self.sqlite_service.marcar_como_procesado(tarea['id'])
                print(f"✅ {tarea['tipo_operacion']} procesado para {tarea['tabla']} id={tarea['id_registro']}")

            except Exception as e:
                print(f"⚠️ Error al procesar tarea {tarea['id']}: {e}")
