from collections import defaultdict
from datetime import datetime
import json
import sqlite3
import uuid
from typing import Dict
from zoneinfo import ZoneInfo
from app.utils.logger import log_error
from config import Config

DB_PATH = Config.DB_PATH

class SQLiteService():
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._asegurar_base_y_tabla()

    def _asegurar_base_y_tabla(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS camaras_frigorifico (
                uuid TEXT PRIMARY KEY,
                id_procesa_app INTEGER, 
                fecha_de_descarga TEXT,
                certificado TEXT,
                sku_tina TEXT,
                sku_talla TEXT,
                peso_bruto REAL,
                tanque TEXT,
                hora_de_marbete TEXT,
                hora_de_pesado TEXT,
                fda TEXT,
                lote_fda TEXT,
                lote_sap TEXT,
                peso_neto REAL,
                tara REAL,
                observaciones TEXT,
                fecha_hora_guardado TIMESTAMP,
                estado INTEGER DEFAULT 0,
                empleado INTEGER
            )
        ''')
        conn.commit()
        conn.close()

    def guardar(self, datos: Dict[str, str]) -> str:
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            nueva_tara_val = datos.get('nueva_tara')
            if nueva_tara_val is not None and str(nueva_tara_val).strip() != '':
                tara_val = float(nueva_tara_val)
            else:
                tara_val = float(datos.get('tara', 0))
            id = str(uuid.uuid4())
            cursor.execute('''
                INSERT INTO camaras_frigorifico (
                    uuid, id_procesa_app, fecha_de_descarga, certificado, sku_tina,
                    sku_talla, peso_bruto, tanque, hora_de_marbete,
                    hora_de_pesado, fda, lote_fda, lote_sap, peso_neto, tara, observaciones, fecha_hora_guardado, empleado
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                id,
                datos.get('id_procesa_app'),
                datos.get('fecha_de_descarga'),
                datos.get('certificado'),
                datos.get('sku_tina'),
                datos.get('sku_talla'),
                float(datos.get('peso_bruto', 0)),
                datos.get('tanque'),
                datos.get('hora_de_marbete'),
                datos.get('hora_de_pesado'),
                datos.get('fda'),
                datos.get('lote_fda'),
                datos.get('lote_sap'),
                float(datos.get('peso_neto', 0)),
                tara_val,
                datos.get('observaciones'),
                datos.get('fecha_hora_guardado'),
                datos.get('nomina'),
            ))
            conn.commit()
            conn.close()
            return id
        except Exception as e:
            print("Error guardando en SQLite:", e)
            raise

    def marcar_como_borrado(self, id: str):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("UPDATE camaras_frigorifico SET estado = 1 WHERE uuid = ?", (id,))
            conn.commit()
            conn.close()
        except Exception as e:
            print("Error al marcar como borrado en SQLite:", e)
            raise

    def _asegurar_tabla_catalogo_de_tina(self):
        """Crea la tabla catalogo_de_tina si no existe."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalogo_de_tina (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT NOT NULL,
                tara INTEGER NOT NULL,
                fecha_hora_guardado TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()

    def consulta_por_grupo(self):
        """Crea la tabla catalogo_de_tina si no existe."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalogo_de_tina (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT NOT NULL,
                tara INTEGER NOT NULL,
                fecha_hora_guardado TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()


    def guardar_catalogo_de_tina(self, datos: list[Dict[str, str]]):
        self._asegurar_tabla_catalogo_de_tina()
        """Vacía e inserta nuevos registros en catalogo_de_tina."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Vaciar tabla antes de insertar
            cursor.execute("DELETE FROM catalogo_de_tina")
            fecha_local = datetime.now(ZoneInfo("America/Mexico_City")).strftime("%Y-%m-%d %H:%M:%S")
            # Insertar datos
            for item in datos:
                cursor.execute(
                    "INSERT INTO catalogo_de_tina (sku, tara, fecha_hora_guardado) VALUES (?, ?, ?)",
                    (item['sku'], int(item['tara']), fecha_local)
                )

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            return False

    def _asegurar_tabla_catalogo_de_talla(self):
        """Crea la tabla catalogo_de_tina si no existe."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalogo_de_talla (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT NOT NULL,
                descripcion TEXT NULL,
                especie TEXT NULL,
                talla TEXT NULL,
                fecha_hora_guardado TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()


    def guardar_catalogo_de_talla(self, datos: list[Dict[str, str]]):
        self._asegurar_tabla_catalogo_de_talla()
        """Vacía e inserta nuevos registros en catalogo_de_talla."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Vaciar tabla antes de insertar
            cursor.execute("DELETE FROM catalogo_de_talla")
            fecha_local = datetime.now(ZoneInfo("America/Mexico_City")).strftime("%Y-%m-%d %H:%M:%S")
            # Insertar datos
            for item in datos:
                cursor.execute(
                    "INSERT INTO catalogo_de_talla (sku, descripcion, especie, talla, fecha_hora_guardado) VALUES (?, ?, ?, ?, ?)",
                    (
                        item.get('sku', ''),
                        item.get('descripcion', ''),
                        item.get('especie', ''),
                        item.get('talla', ''),
                        fecha_local
                    )
                )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            return False

    def _asegurar_tabla_catalogo_de_barcos(self):
        """Crea la tabla catalogo_de_barcos si no existe."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS catalogo_de_barcos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                inicial TEXT NULL,
                descripcion TEXT NULL,
                fecha_hora_guardado TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
        conn.close()

    def _asegurar_tablas_remisiones(self):
        """Crea las tablas remisiones_cabecera y remisiones_cuerpo si no existen."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Tabla de cabecera
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS remisiones_cabecera (
                uuid TEXT PRIMARY KEY,
                carga TEXT,
                cantidad_solicitada REAL,
                folio TEXT,
                cliente TEXT,
                numero_sello TEXT,
                placas_contenedor TEXT,
                factura TEXT,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        ''')

        # Tabla de cuerpo
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS remisiones_cuerpo (
                uuid TEXT PRIMARY KEY,
                id_remision TEXT,
                sku_tina TEXT,
                sku_talla TEXT,
                tara REAL,
                peso_neto REAL,
                merma REAL,
                lote TEXT,
                tanque TEXT,
                peso_marbete REAL,
                peso_bascula REAL,
                peso_neto_devolucion REAL DEFAULT NULL,
                peso_bruto_devolucion REAL DEFAULT NULL,
                observaciones TEXT,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (id_remision) REFERENCES remisiones_cabecera(uuid)
            );
        ''')

        conn.commit()
        conn.close()

    def guardar_remision(self, data):
        self._asegurar_tablas_remisiones()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        fecha_local = datetime.now(ZoneInfo("America/Mexico_City")).strftime("%Y-%m-%d %H:%M:%S")
        remision_uuid = str(uuid.uuid4())

        try:
            # Paso 1: Buscar si la cabecera ya existe
            cursor.execute("""
                SELECT uuid FROM remisiones_cabecera
                WHERE carga = ? AND cantidad_solicitada = ? 
                AND DATE(fecha_creacion) = DATE(?)
            """, (
                data.get("carga"),
                float(data.get("cantidad_solicitada")) if data.get("cantidad_solicitada") else 0,
                fecha_local
            ))
            row = cursor.fetchone()

            if row:
                # Ya existe la cabecera
                id_remision = row[0]

                # Construir update dinámico SOLO con los campos no vacíos
                campos_a_actualizar = []
                valores = []

                for campo in ["folio", "cliente", "numero_sello", "placas_contenedor", "factura"]:
                    valor = data.get(campo)
                    if valor:  # Si no está vacío o None
                        campos_a_actualizar.append(f"{campo} = ?")
                        valores.append(valor)

                if campos_a_actualizar:
                    sql_update = f"""
                        UPDATE remisiones_cabecera 
                        SET {", ".join(campos_a_actualizar)}
                        WHERE uuid = ?
                    """
                    valores.append(id_remision)
                    cursor.execute(sql_update, tuple(valores))

            else:
                id_remision = remision_uuid
                # Crear nueva cabecera
                cursor.execute("""
                    INSERT INTO remisiones_cabecera (
                        uuid, carga, cantidad_solicitada, fecha_creacion,
                        folio, cliente, numero_sello, placas_contenedor, factura
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    id_remision,
                    data.get("carga"),
                    float(data.get("cantidad_solicitada")) if data.get("cantidad_solicitada") else 0,
                    fecha_local,
                    data.get("folio"),
                    data.get("cliente"),
                    data.get("numero_sello"),
                    data.get("placas_contenedor"),
                    data.get("factura"),
                ))

            # Paso 2: Insertar en el CUERPO
            cuerpo_uuid = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO remisiones_cuerpo (
                    uuid, id_remision, sku_tina, sku_talla, tara, peso_neto, merma,
                    lote, tanque, peso_marbete, peso_bascula,
                    peso_neto_devolucion, peso_bruto_devolucion, observaciones
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                cuerpo_uuid,
                id_remision,
                data.get("sku_tina"),
                data.get("sku_talla"),
                float(data.get("tara")) if data.get("tara") else 0,
                float(data.get("peso_neto")) if data.get("peso_neto") else 0,
                float(data.get("merma")) if data.get("merma") else 0,
                data.get("lote"),
                data.get("tanque"),
                float(data.get("peso_marbete")) if data.get("peso_marbete") else 0,
                float(data.get("peso_bascula")) if data.get("peso_bascula") else 0,
                float(data.get("peso_neto_devolucion")) if data.get("peso_neto_devolucion") else None,
                float(data.get("peso_bruto_devolucion")) if data.get("peso_bruto_devolucion") else None,
                data.get("observaciones")
            ))

            conn.commit()
            return id_remision

        except Exception as e:
            conn.rollback()
            raise e

        finally:
            conn.close()

    def cargas_del_dia(self):
        self._asegurar_tablas_remisiones()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        today_date_str = datetime.now().strftime('%Y-%m-%d')
        cursor.execute("""
            SELECT rc.uuid, rc.carga, rc.cantidad_solicitada, rc.folio, rc.cliente, rc.numero_sello, rc.placas_contenedor, rc.factura, rc.fecha_creacion,
                rcu.uuid AS cuerpo_id, rcu.sku_tina, rcu.sku_talla, rcu.tara, rcu.peso_neto,
                rcu.merma, rcu.lote, rcu.tanque, rcu.peso_marbete, rcu.peso_bascula,
                rcu.peso_neto_devolucion, rcu.peso_bruto_devolucion, rcu.observaciones
            FROM remisiones_cabecera rc
            LEFT JOIN remisiones_cuerpo rcu ON rc.uuid = rcu.id_remision
            WHERE DATE(rc.fecha_creacion) = ?
            ORDER BY rcu.fecha_creacion
        """, (today_date_str,))

        results = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]

        # Agrupar cabecera + cuerpo
        data = {}
        for row in results:
            row_dict = dict(zip(column_names, row))
            cabecera_id = row_dict["uuid"]

            if cabecera_id not in data:
                data[cabecera_id] = {
                    "uuid": row_dict["uuid"],
                    "carga": row_dict["carga"],
                    "cantidad_solicitada": row_dict["cantidad_solicitada"],
                    "folio": row_dict["folio"],
                    "cliente": row_dict["cliente"],
                    "numero_sello": row_dict["numero_sello"],
                    "placas_contenedor": row_dict["placas_contenedor"],
                    "factura": row_dict["factura"],
                    "fecha_creacion": row_dict["fecha_creacion"],
                    "detalles": []
                }

            if row_dict["cuerpo_id"]:
                data[cabecera_id]["detalles"].append({
                    "cuerpo_id": row_dict["cuerpo_id"],
                    "sku_tina": row_dict["sku_tina"],
                    "sku_talla": row_dict["sku_talla"],
                    "tara": row_dict["tara"],
                    "peso_neto": row_dict["peso_neto"],
                    "merma": row_dict["merma"],
                    "lote": row_dict["lote"],
                    "tanque": row_dict["tanque"],
                    "peso_marbete": row_dict["peso_marbete"],
                    "peso_bascula": row_dict["peso_bascula"],
                    "peso_neto_devolucion": row_dict["peso_neto_devolucion"],
                    "peso_bruto_devolucion": row_dict["peso_bruto_devolucion"],
                    "observaciones": row_dict["observaciones"]
                })
        conn.close()
        return json.dumps(list(data.values()), ensure_ascii=False)

    def remisiones_del_dia_por_carga(self, carga, cantidad_solicitada):
        self._asegurar_tablas_remisiones()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        today_date_str = datetime.now().strftime('%Y-%m-%d')

        cursor.execute("""
            SELECT rc.uuid, rc.carga, rc.cantidad_solicitada, rc.folio, rc.cliente, rc.numero_sello, rc.placas_contenedor, rc.factura, rc.fecha_creacion,
                rcu.uuid AS cuerpo_id, rcu.sku_tina, rcu.sku_talla, rcu.tara, rcu.peso_neto,
                rcu.merma, rcu.lote, rcu.tanque, rcu.peso_marbete, rcu.peso_bascula,
                rcu.peso_neto_devolucion, rcu.peso_bruto_devolucion, rcu.observaciones
            FROM remisiones_cabecera rc
            LEFT JOIN remisiones_cuerpo rcu ON rc.uuid = rcu.id_remision
            WHERE DATE(rc.fecha_creacion) = ?
            AND rc.carga = ?
            AND rc.cantidad_solicitada = ?
            ORDER BY rc.fecha_creacion
        """, (today_date_str, carga, cantidad_solicitada))

        results = cursor.fetchall()
        column_names = [description[0] for description in cursor.description]

        data = None
        for row in results:
            row_dict = dict(zip(column_names, row))
            if data is None:
                data = {
                    "uuid": row_dict["uuid"],
                    "carga": row_dict["carga"],
                    "cantidad_solicitada": row_dict["cantidad_solicitada"],
                    "folio": row_dict["folio"],
                    "cliente": row_dict["cliente"],
                    "numero_sello": row_dict["numero_sello"],
                    "placas_contenedor": row_dict["placas_contenedor"],
                    "factura": row_dict["factura"],
                    "fecha_creacion": row_dict["fecha_creacion"],
                    "detalles": []
                }

            if row_dict["cuerpo_id"]:
                data["detalles"].append({
                    "cuerpo_id": row_dict["cuerpo_id"],
                    "sku_tina": row_dict["sku_tina"],
                    "sku_talla": row_dict["sku_talla"],
                    "tara": row_dict["tara"],
                    "peso_neto": row_dict["peso_neto"],
                    "merma": row_dict["merma"],
                    "lote": row_dict["lote"],
                    "tanque": row_dict["tanque"],
                    "peso_marbete": row_dict["peso_marbete"],
                    "peso_bascula": row_dict["peso_bascula"],
                    "peso_neto_devolucion": row_dict["peso_neto_devolucion"],
                    "peso_bruto_devolucion": row_dict["peso_bruto_devolucion"],
                    "observaciones": row_dict["observaciones"]
                })

        conn.close()
        return json.dumps(data if data else {}, ensure_ascii=False)

    def todas_las_remisiones(self):
        """Devuelve todas las remisiones con sus detalles (cabecera + cuerpo)."""
        self._asegurar_tablas_remisiones()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT rc.uuid, rc.carga, rc.cantidad_solicitada, rc.folio, rc.cliente, 
                rc.numero_sello, rc.placas_contenedor, rc.factura, rc.fecha_creacion,
                rcu.uuid AS cuerpo_id, rcu.sku_tina, rcu.sku_talla, rcu.tara, rcu.peso_neto,
                rcu.merma, rcu.lote, rcu.tanque, rcu.peso_marbete, rcu.peso_bascula,
                rcu.peso_neto_devolucion, rcu.peso_bruto_devolucion, rcu.observaciones
            FROM remisiones_cabecera rc
            LEFT JOIN remisiones_cuerpo rcu ON rc.uuid = rcu.id_remision
            ORDER BY rc.fecha_creacion DESC, rcu.fecha_creacion
        """)

        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        data = {}
        for row in results:
            row_dict = dict(zip(column_names, row))
            cabecera_id = row_dict["uuid"]

            if cabecera_id not in data:
                data[cabecera_id] = {
                    "uuid": row_dict["uuid"],
                    "carga": row_dict["carga"],
                    "cantidad_solicitada": row_dict["cantidad_solicitada"],
                    "folio": row_dict["folio"],
                    "cliente": row_dict["cliente"],
                    "numero_sello": row_dict["numero_sello"],
                    "placas_contenedor": row_dict["placas_contenedor"],
                    "factura": row_dict["factura"],
                    "fecha_creacion": row_dict["fecha_creacion"],
                    "detalles": []
                }

            if row_dict["cuerpo_id"]:
                data[cabecera_id]["detalles"].append({
                    "cuerpo_id": row_dict["cuerpo_id"],
                    "sku_tina": row_dict["sku_tina"],
                    "sku_talla": row_dict["sku_talla"],
                    "tara": row_dict["tara"],
                    "peso_neto": row_dict["peso_neto"],
                    "merma": row_dict["merma"],
                    "lote": row_dict["lote"],
                    "tanque": row_dict["tanque"],
                    "peso_marbete": row_dict["peso_marbete"],
                    "peso_bascula": row_dict["peso_bascula"],
                    "peso_neto_devolucion": row_dict["peso_neto_devolucion"],
                    "peso_bruto_devolucion": row_dict["peso_bruto_devolucion"],
                    "observaciones": row_dict["observaciones"]
                })

        conn.close()
        return list(data.values())

    def obtener_remisiones_cuerpo_hoy(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        today_date_str = datetime.now().strftime('%Y-%m-%d')
        cur.execute("""
            SELECT rc.*
            FROM remisiones_cuerpo rc
            INNER JOIN remisiones_cabecera rch ON rc.id_remision = rch.uuid
            WHERE DATE(rch.fecha_creacion) = ?
        """, (today_date_str,))
        rows = [dict(row) for row in cur.fetchall()]
        conn.close()
        return rows

    def remisiones_por_rango(self, fecha_inicio:str, fecha_fin:str):
        """
        Devuelve todas las remisiones cuya fecha_creacion esté en [fecha_inicio, fecha_fin)
        Formatos esperados: 'YYYY-MM-DD HH:MM:SS'
        """
        self._asegurar_tablas_remisiones()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT rc.uuid, rc.carga, rc.cantidad_solicitada, rc.folio, rc.cliente, 
                    rc.numero_sello, rc.placas_contenedor, rc.factura, rc.fecha_creacion,
                    rcu.uuid AS cuerpo_id, rcu.sku_tina, rcu.sku_talla, rcu.tara, rcu.peso_neto,
                    rcu.merma, rcu.lote, rcu.tanque, rcu.peso_marbete, rcu.peso_bascula,
                    rcu.peso_neto_devolucion, rcu.peso_bruto_devolucion, rcu.observaciones
                FROM remisiones_cabecera rc
                LEFT JOIN remisiones_cuerpo rcu ON rc.uuid = rcu.id_remision
                WHERE rc.fecha_creacion >= ? AND rc.fecha_creacion < ?
                ORDER BY rcu.fecha_creacion
            """, (fecha_inicio, fecha_fin))
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

            data = {}
            for row in rows:
                row_dict = dict(zip(column_names, row))
                cabecera_id = row_dict["uuid"]

                if cabecera_id not in data:
                    data[cabecera_id] = {
                        "uuid": row_dict["uuid"],
                        "carga": row_dict["carga"],
                        "cantidad_solicitada": row_dict["cantidad_solicitada"],
                        "folio": row_dict["folio"],
                        "cliente": row_dict["cliente"],
                        "numero_sello": row_dict["numero_sello"],
                        "placas_contenedor": row_dict["placas_contenedor"],
                        "factura": row_dict["factura"],
                        "fecha_creacion": row_dict["fecha_creacion"],
                        "detalles": []
                    }

                if row_dict["cuerpo_id"]:
                    data[cabecera_id]["detalles"].append({
                        "cuerpo_id": row_dict["cuerpo_id"],
                        "sku_tina": row_dict["sku_tina"],
                        "sku_talla": row_dict["sku_talla"],
                        "tara": row_dict["tara"],
                        "peso_neto": row_dict["peso_neto"],
                        "merma": row_dict["merma"],
                        "lote": row_dict["lote"],
                        "tanque": row_dict["tanque"],
                        "peso_marbete": row_dict["peso_marbete"],
                        "peso_bascula": row_dict["peso_bascula"],
                        "peso_neto_devolucion": row_dict["peso_neto_devolucion"],
                        "peso_bruto_devolucion": row_dict["peso_bruto_devolucion"],
                        "observaciones": row_dict["observaciones"]
                    })

            conn.close()
            return list(data.values())
        finally:
            conn.close()


    def actualizar_campo_remision(self, tabla: str, id_local: str, campo: str, valor):
        """Actualiza un solo campo editable de un registro de remisiones."""
        if tabla not in ["cabecera", "cuerpo"]:
            raise ValueError("Tabla inválida")

        if tabla == "cabecera":
            campos_editables = ["carga", "cantidad_solicitada","folio", "cliente", "numero_sello", "placas_contenedor", "factura"]
            tabla_sql = "remisiones_cabecera"
            id_campo = "uuid"
        else:
            campos_editables = [
                "sku_tina", "sku_talla", "tara", "peso_neto", "merma", "lote",
                "tanque", "peso_marbete", "peso_bascula",
                "peso_neto_devolucion", "peso_bruto_devolucion", "observaciones"
            ]
            tabla_sql = "remisiones_cuerpo"
            id_campo = "uuid"

        if campo not in campos_editables:
            raise ValueError(f"Campo no editable: {campo}")

        valor_sql = valor if valor not in (None, '') else None

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"UPDATE {tabla_sql} SET {campo} = ? WHERE {id_campo} = ?", (valor_sql, id_local))
        conn.commit()
        conn.close()

    def guardar_catalogo_barcos(self, datos: list[dict]):
        """Vacía e inserta nuevos registros en catalogo_de_barcos."""
        self._asegurar_tabla_catalogo_de_barcos()
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM catalogo_de_barcos")
            fecha_local = datetime.now(ZoneInfo("America/Mexico_City")).strftime("%Y-%m-%d %H:%M:%S")
            for item in datos:
                cursor.execute("""
                    INSERT INTO catalogo_de_barcos (
                        inicial, descripcion, fecha_hora_guardado
                    ) VALUES (?, ?, ?)
                """, (
                    item.get('inicial', ''),
                    item.get('descripcion', ''),
                    fecha_local
                ))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error al guardar catalogo_de_barcos: {e}")
            log_error(f"Error al guardar el sqlite: {e}", archivo=__file__)
            return False

    def actualizar_campo(self, id_local: int, campo: str, valor):
        """Actualiza un solo campo editable de un registro local."""
        try:
            # solo permite campos que son editables
            campos_editables = ['sku_tina', 'sku_talla', 'peso_bruto', 'tara', 'tanque', 'peso_neto']
            if campo not in campos_editables:
                raise ValueError(f"Campo no editable: {campo}")

            # Si el valor es None o vacío, usar NULL
            valor_sql = valor if valor not in (None, '') else None

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"UPDATE camaras_frigorifico SET {campo} = ? WHERE uuid = ?", (valor_sql, id_local))
            conn.commit()
            conn.close()

        except Exception as e:
            log_error(f"Error actualizando campo {campo} del registro {id_local}: {e}", archivo=__file__)
            raise

    def obtener_registro_por_id(self, id_local: str) -> dict:
        """Obtiene todos los campos del registro como diccionario."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM camaras_frigorifico WHERE uuid = ?", (id_local,))
            row = cursor.fetchone()
            columnas = [col[0] for col in cursor.description]
            conn.close()
            if row:
                return dict(zip(columnas, row))
            else:
                raise Exception(f"Registro {id_local} no encontrado")
        except Exception as e:
            log_error(f"Error obteniendo registro {id_local}: {e}", archivo=__file__)
            raise

    def obtener_ultimos_13(self):
        """Devuelve los últimos 13 registros activos (estado=0)."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
                SELECT cf.uuid,
                    cf.id_procesa_app,
                    cf.sku_tina,
                    cf.sku_talla,
                    CASE
                        WHEN cf.sku_talla LIKE 'P%' OR cf.sku_talla LIKE 'p%' 
                                THEN IFNULL(ct.especie || ' ' || ct.talla, '')
                        WHEN cf.sku_talla LIKE 'B%' OR cf.sku_talla LIKE 'b%' 
                                THEN IFNULL(ct.descripcion, '')
                        ELSE IFNULL(ct.descripcion, '')
                    END AS talla_descripcion,
                    cf.tara,
                    cf.peso_bruto,
                    cf.peso_neto,
                    cf.lote_fda,
                    cf.fecha_hora_guardado,
                    cf.tanque
                FROM camaras_frigorifico cf
                LEFT JOIN catalogo_de_talla ct
                    ON cf.sku_talla = ct.sku
                WHERE cf.estado = 0
                ORDER BY cf.uuid DESC
                LIMIT 13
            """)

            filas = cursor.fetchall()
            conn.close()

            # Convertir Row objects a dict
            return {"datos": [dict(fila) for fila in filas]}
        except Exception as e:
            return {"error": "Error en la consulta SQL"}

    def buscar_barco(self, letra: str) -> str:
        """
        Devuelve el nombre del barco en caso de encontrarse
        """
        self._asegurar_tabla_catalogo_de_barcos()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT descripcion FROM catalogo_de_barcos WHERE inicial = ?", (letra,))
        fila = cursor.fetchone()
        conn.close()

        if fila:
            return fila[0]
        else:
            return "No encontrado"

    def obtener_peso_tara(self, sku: str) -> str:
        """
        Devuelve el peso de la tina en formato "Tara: XXX Kg".
        Si no se encuentra, devuelve "Tara: desconocida".
        """
        self._asegurar_tabla_catalogo_de_tina()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT tara FROM catalogo_de_tina WHERE sku = ?", (sku,))
        fila = cursor.fetchone()
        conn.close()

        if fila:
            return f"Tara: {fila[0]} Kg"
        else:
            return "Tara: desconocida"
        
    def descripcion_talla(self, sku: str) -> str:
        """
        Devuelve descripcion de talla
        """
        self._asegurar_tabla_catalogo_de_talla()
        if sku is None:
            return ""
        if sku.startswith('P') or sku.startswith('p'):
            query = "SELECT especie || ' ' || talla AS descripcion FROM catalogo_de_talla WHERE sku = ?"
        elif sku.startswith('B') or sku.startswith('b'):
            query = "SELECT descripcion FROM catalogo_de_talla WHERE sku = ?"
        else:
            query = "SELECT descripcion FROM catalogo_de_talla WHERE sku = ?"
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(query, (sku,))
        fila = cursor.fetchone()
        conn.close()

        if fila:
            return fila[0]
        else:
            return ""

    def buscar_talla_por_sku(self, sku_talla):
        """
        Devuelve exactamente lo que devuelve el PHP:
        - Si encuentra el SKU, devuelve 'descripcion especie talla Kg'
        - Si no lo encuentra, devuelve 'SKU talla no encontrada'
        """
        salida = ""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT sku, descripcion, especie, talla
            FROM catalogo_de_talla
            WHERE sku = ?
        """, (sku_talla,))

        fila = cursor.fetchone()

        if fila:
            sku, descripcion, especie, talla = fila
            salida = f"{descripcion} {especie} {talla} Kg"
        else:
            salida = "SKU talla no encontrada"

        conn.close()
        return salida
    
    def buscar_peso_por_lote(self, lote_fda: str) -> float:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT SUM(peso_neto) AS total_peso_neto
            FROM camaras_frigorifico
            WHERE estado = 0
            AND lote_fda LIKE ?
        """, (f"%{lote_fda}%",))

        resultado = cursor.fetchone()
        conn.close()

        return resultado[0] if resultado and resultado[0] is not None else 0
        
    def obtener_reportes(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # ---- Totales por lote ----
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN lote_fda LIKE 'P%' THEN substr(lote_fda, 1, 10)
                    ELSE substr(lote_fda, 1, 6)
                END AS lote,
                SUM(peso_neto) AS total_peso_neto
            FROM camaras_frigorifico
            WHERE estado = 0
            GROUP BY lote
        """)
        totales_por_lote = [
            {"lote": row[0], "total_peso_neto": row[1]} for row in cursor.fetchall()
        ]

        # ---- Tabla por día + lote + talla ----
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN lote_fda LIKE 'P%' THEN substr(lote_fda, 1, 10)
                    ELSE substr(lote_fda, 1, 6)
                END AS lote,
                DATE(fecha_de_descarga) AS dia,
                sku_talla,
                SUM(peso_neto) AS total_peso
            FROM camaras_frigorifico
            WHERE estado = 0
            GROUP BY lote, dia, sku_talla
            ORDER BY lote, dia
        """)
        rows = cursor.fetchall()

        # Organizar la info
        data_por_lote = defaultdict(lambda: defaultdict(dict))  
        dias_por_lote = defaultdict(set)  

        for lote, dia, talla, total in rows:
            data_por_lote[lote][talla][dia] = total
            dias_por_lote[lote].add(dia)

        # Armar la tabla estilo pivot
        tablas = {}
        for lote, tallas_data in data_por_lote.items():
            dias_ordenados = sorted([d for d in dias_por_lote[lote] if d is not None])
            tabla = []

            for talla, valores_por_dia in tallas_data.items():
                fila = {"talla": talla}
                for d in dias_ordenados:
                    fila[d] = valores_por_dia.get(d, 0)
                tabla.append(fila)

            tablas[lote] = {
                "dias": dias_ordenados,
                "tabla": tabla
            }

        # ---- Sección "todo_sobre_ultimo_lote" ----
        cursor.execute("""
            SELECT uuid,id_procesa_app,sku_tina,sku_talla,peso_bruto,tara,peso_neto,lote_fda,tanque,fecha_hora_guardado
            FROM camaras_frigorifico
            WHERE 
                CASE 
                    WHEN lote_fda LIKE 'P%' THEN substr(lote_fda, 1, 10)
                    ELSE substr(lote_fda, 1, 6)
                END = (
                    SELECT 
                        CASE 
                            WHEN lote_fda LIKE 'P%' THEN substr(lote_fda, 1, 10)
                            ELSE substr(lote_fda, 1, 6)
                        END AS lote
                    FROM camaras_frigorifico
                    ORDER BY fecha_de_descarga DESC
                    LIMIT 1
                )
                AND estado = 0
        """)
        filas_ultimo_lote = cursor.fetchall()
        columnas = [desc[0] for desc in cursor.description]

        todo_sobre_ultimo_lote = [dict(zip(columnas, fila)) for fila in filas_ultimo_lote]

        # query_anterior_para_las_descripciones = "SELECT sku,especie || ' - ' || talla as descripcion FROM catalogo_de_talla"
        # ---- Sección "tallas" ----
        cursor.execute("""
            SELECT 
                sku,
                CASE 
                    WHEN sku LIKE 'B%' THEN descripcion
                    ELSE descripcion || ' - ' || talla
                END AS descripcion
            FROM catalogo_de_talla
        """)
        tallas = cursor.fetchall()
        columnas = [desc[0] for desc in cursor.description]
        tallas_dict = {sku: descripcion for sku, descripcion in tallas}

        conn.close()

        # -------- Construir JSON final ----------
        resultado = {
            "totales_por_lote": totales_por_lote,
            "detalle_por_lote": tablas,
            "todo_sobre_ultimo_lote": todo_sobre_ultimo_lote,
            "tallas": tallas_dict
        }

        return resultado