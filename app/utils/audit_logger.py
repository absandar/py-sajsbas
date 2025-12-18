import uuid
import json
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo


class AuditLogger:
    @staticmethod
    def log_update(
        conn: sqlite3.Connection,
        tabla: str,
        registro_id: str,
        campo: str,
        valor_anterior,
        valor_nuevo,
        usuario_id: str | None = None
    ):
        """
        Registra un cambio UPDATE en audit_update_log.
        Solo inserta si el valor realmente cambia.
        """

        # No registrar si no hay cambio real
        if valor_anterior == valor_nuevo:
            return

        conn.execute("""
            INSERT INTO audit_update_log (
                uuid,
                tabla,
                registro_id,
                campo,
                valor_anterior,
                valor_nuevo,
                usuario_id,
                fecha_creacion
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(uuid.uuid4()),
            tabla,
            registro_id,
            campo,
            json.dumps(valor_anterior, ensure_ascii=False),
            json.dumps(valor_nuevo, ensure_ascii=False),
            usuario_id,
            datetime.now(ZoneInfo("America/Mexico_City")).strftime("%Y-%m-%d %H:%M:%S")
        ))
