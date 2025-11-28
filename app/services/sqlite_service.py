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
        """Crea las tablas remisiones_general, remisiones_cabecera y remisiones_cuerpo si no existen."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # === Nivel 1: Remisión general (una por día) ===
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS remisiones_general (
                uuid TEXT PRIMARY KEY,
                folio TEXT,
                cliente TEXT,
                numero_sello TEXT,
                placas_contenedor TEXT,
                factura TEXT,
                observaciones TEXT,
                fecha_produccion TEXT,
                borrado INTEGER DEFAULT 0,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        ''')

        # === Nivel 2: Cargas (una o varias por remisión general) ===
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS remisiones_cabecera (
                uuid TEXT PRIMARY KEY,
                id_remision_general TEXT NOT NULL,
                carga TEXT,
                cantidad_solicitada REAL,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                borrado INTEGER DEFAULT 0,
                FOREIGN KEY (id_remision_general) REFERENCES remisiones_general(uuid)
            );
        ''')

        # === Nivel 3: Cuerpo (detalle por tina) ===
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS remisiones_cuerpo (
                uuid TEXT PRIMARY KEY,
                id_remision TEXT NOT NULL,
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
                is_msc INTEGER DEFAULT 0 CHECK (is_msc IN (0, 1)), 
                is_sensorial INTEGER DEFAULT 0 CHECK (is_sensorial IN (0, 1)), 
                observaciones TEXT,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                borrado INTEGER DEFAULT 0,
                FOREIGN KEY (id_remision) REFERENCES remisiones_cabecera(uuid)
            );
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS remisiones_retallados (
                uuid TEXT PRIMARY KEY,
                id_remision_general TEXT NOT NULL,
                sku_tina TEXT,
                sku_talla TEXT,
                lote TEXT,
                tara REAL,
                peso_bascula REAL,
                peso_neto REAL,
                observaciones TEXT,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                borrado INTEGER DEFAULT 0,
                FOREIGN KEY (id_remision_general) REFERENCES remisiones_general(uuid)
            );
        ''')
        cursor.execute('''
            CREATE TRIGGER IF NOT EXISTS trg_actualizar_peso_neto
            AFTER UPDATE OF peso_bascula, tara ON remisiones_cuerpo
            FOR EACH ROW
            BEGIN
                UPDATE remisiones_cuerpo
                SET peso_neto = 
                    CASE 
                        WHEN NEW.peso_bascula IS NOT NULL AND NEW.tara IS NOT NULL 
                        THEN (NEW.peso_bascula - NEW.tara)
                        ELSE NULL
                    END
                WHERE uuid = NEW.uuid;
            END;
        ''')
        cursor.execute('''
            CREATE TRIGGER IF NOT EXISTS trg_actualizar_merma
            AFTER UPDATE OF peso_marbete, peso_neto ON remisiones_cuerpo
            FOR EACH ROW
            BEGIN
                UPDATE remisiones_cuerpo
                SET merma = 
                    CASE 
                        WHEN NEW.peso_marbete IS NOT NULL AND NEW.peso_neto IS NOT NULL 
                        THEN (NEW.peso_marbete - NEW.peso_neto)
                        ELSE NULL
                    END
                WHERE uuid = NEW.uuid;
            END;
        ''')
        conn.commit()
        conn.close()

    def guardar_remision(self, data):
        """
        Guarda una nueva remisión general (si no existe), la carga y el detalle (tina).
        """
        print(data)
        self._asegurar_tablas_remisiones()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        fecha_local = datetime.now(ZoneInfo("America/Mexico_City")).strftime("%Y-%m-%d %H:%M:%S")
        general_uuid = None
        remision_uuid = None

        try:
            # === Paso 1: Buscar o crear remisión_general del día ===
            today_str = datetime.now(ZoneInfo("America/Mexico_City")).strftime("%Y-%m-%d")

            cursor.execute("""
                SELECT uuid FROM remisiones_general
                WHERE DATE(fecha_creacion) = ? AND borrado = 0
            """, (today_str,))
            row = cursor.fetchone()

            if row:
                general_uuid = row[0]
                # Actualizar datos generales si vienen nuevos
                campos_a_actualizar = []
                valores = []
                for campo in ["folio", "cliente", "numero_sello", "placas_contenedor", "fecha_produccion", "factura"]:
                    valor = data.get(campo)
                    if valor not in (None, ''):
                        campos_a_actualizar.append(f"{campo} = ?")
                        valores.append(valor)
                if campos_a_actualizar:
                    print(campos_a_actualizar)
                    sql_update = f"""
                        UPDATE remisiones_general
                        SET {", ".join(campos_a_actualizar)}
                        WHERE uuid = ?
                    """
                    valores.append(general_uuid)
                    cursor.execute(sql_update, tuple(valores))
            else:
                general_uuid = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO remisiones_general (
                        uuid, folio, cliente, numero_sello, placas_contenedor, fecha_produccion, factura, fecha_creacion
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    general_uuid,
                    data.get("folio"),
                    data.get("cliente"),
                    data.get("numero_sello"),
                    data.get("placas_contenedor"),
                    data.get("fecha_produccion"),
                    data.get("factura"),
                    fecha_local
                ))

            # === Paso 2: Buscar o crear la carga (remisiones_cabecera) ===
            cursor.execute("""
                SELECT uuid FROM remisiones_cabecera
                WHERE id_remision_general = ?
                AND carga = ?
                AND cantidad_solicitada = ?
                AND DATE(fecha_creacion) = DATE(?)
                AND borrado = 0
            """, (
                general_uuid,
                data.get("carga"),
                float(data.get("cantidad_solicitada")) if data.get("cantidad_solicitada") else 0,
                fecha_local
            ))
            row = cursor.fetchone()

            if row:
                remision_uuid = row[0]
            else:
                remision_uuid = str(uuid.uuid4())
                cursor.execute("""
                    INSERT INTO remisiones_cabecera (
                        uuid, id_remision_general, carga, cantidad_solicitada, fecha_creacion
                    )
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    remision_uuid,
                    general_uuid,
                    data.get("carga"),
                    float(data.get("cantidad_solicitada")) if data.get("cantidad_solicitada") else 0,
                    fecha_local
                ))

            # === Paso 3: Insertar detalle (remisiones_cuerpo) ===
            cuerpo_uuid = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO remisiones_cuerpo (
                    uuid, id_remision, sku_tina, sku_talla, tara, peso_neto, merma,
                    lote, tanque, peso_marbete, peso_bascula,
                    peso_neto_devolucion, peso_bruto_devolucion, observaciones, is_msc, is_sensorial, fecha_creacion
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                cuerpo_uuid,
                remision_uuid,
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
                data.get("observaciones"),
                data.get("is_msc"),
                data.get("is_sensorial"),
                fecha_local
            ))

            conn.commit()
            return remision_uuid

        except Exception as e:
            conn.rollback()
            raise e

        finally:
            conn.close()

    def obtener_fecha_produccion_hoy(self):
        """
        Devuelve la fecha_produccion del día de hoy en formato YYMMDD.
        Si no existe, devuelve "".
        """
        self._asegurar_tablas_remisiones()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        today_date_str = datetime.now().strftime('%Y-%m-%d')

        cursor.execute("""
            SELECT fecha_produccion
            FROM remisiones_general
            WHERE DATE(fecha_creacion) = ? AND borrado = 0
            ORDER BY fecha_creacion DESC
            LIMIT 1
        """, (today_date_str,))

        row = cursor.fetchone()
        conn.close()

        if not row or not row[0]:
            return ""

        try:
            fecha = datetime.strptime(row[0], "%Y-%m-%d")
            return fecha.strftime("%y%m%d")   # ← FORMATO YYMMDD
        except:
            return ""

    def cargas_del_dia(self):
        """
        Devuelve la remisión general del día actual,
        con todas sus cargas y detalles.
        """
        self._asegurar_tablas_remisiones()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        today_date_str = datetime.now().strftime('%Y-%m-%d')

        # === Paso 1: Obtener la remisión general del día ===
        cursor.execute("""
            SELECT uuid, folio, cliente, numero_sello, placas_contenedor, fecha_produccion, factura, observaciones as observaciones_cabecera, fecha_creacion
            FROM remisiones_general
            WHERE DATE(fecha_creacion) = ? AND borrado = 0
            ORDER BY fecha_creacion DESC
        """, (today_date_str,))
        general = cursor.fetchone()

        if not general:
            conn.close()
            return json.dumps({}, ensure_ascii=False)

        general_columns = [desc[0] for desc in cursor.description]
        remision_general = dict(zip(general_columns, general))

        # === Paso 2: Obtener todas las cargas asociadas a la remisión general ===
        cursor.execute("""
            SELECT uuid, carga, cantidad_solicitada, fecha_creacion
            FROM remisiones_cabecera
            WHERE id_remision_general = ? AND borrado = 0
            ORDER BY fecha_creacion
        """, (remision_general["uuid"],))
        cargas = cursor.fetchall()

        cargas_columns = [desc[0] for desc in cursor.description]
        cargas_dict = []

        # === Paso 3: Por cada carga, obtener sus detalles ===
        for carga_row in cargas:
            carga_dict = dict(zip(cargas_columns, carga_row))

            cursor.execute("""
                SELECT uuid AS cuerpo_id, sku_tina, sku_talla, tara, peso_neto, merma,
                    lote, tanque, peso_marbete, peso_bascula,
                    peso_neto_devolucion, peso_bruto_devolucion, observaciones, is_msc, is_sensorial
                FROM remisiones_cuerpo
                WHERE id_remision = ? AND borrado = 0
                ORDER BY fecha_creacion
            """, (carga_dict["uuid"],))
            detalles = cursor.fetchall()

            if detalles:
                detalles_cols = [d[0] for d in cursor.description]
                carga_dict["detalles"] = [dict(zip(detalles_cols, d)) for d in detalles]
            else:
                carga_dict["detalles"] = []

            cargas_dict.append(carga_dict)

        # === Paso 4: Obtener los retallados asociados a la remisión general ===
        cursor.execute("""
            SELECT uuid, id_remision_general, sku_tina, sku_talla, lote, tara, peso_bascula, peso_neto, observaciones, fecha_creacion
            FROM remisiones_retallados
            WHERE id_remision_general = ? AND borrado = 0
            ORDER BY fecha_creacion
        """, (remision_general["uuid"],))
        retallados = cursor.fetchall()

        # === Armar estructura final ===
        remision_general["cargas"] = cargas_dict
        
        if retallados:
            retallados_cols = [d[0] for d in cursor.description]
            remision_general["retallados"] = [dict(zip(retallados_cols, r)) for r in retallados]
        else:
            remision_general["retallados"] = []

        conn.close()
        return json.dumps(remision_general, ensure_ascii=False)

    def remisiones_del_dia_por_carga(self, carga, cantidad_solicitada):
        """
        Devuelve la remisión general del día actual con una carga específica
        (identificada por carga y cantidad_solicitada) y sus detalles.
        """
        self._asegurar_tablas_remisiones()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        today_date_str = datetime.now().strftime('%Y-%m-%d')

        # === Paso 1: Obtener la remisión general del día ===
        cursor.execute("""
            SELECT uuid, folio, cliente, numero_sello, placas_contenedor, factura, observaciones, fecha_creacion
            FROM remisiones_general
            WHERE DATE(fecha_creacion) = ? AND borrado = 0
            ORDER BY fecha_creacion DESC
            LIMIT 1
        """, (today_date_str,))
        general = cursor.fetchone()

        if not general:
            conn.close()
            return json.dumps({}, ensure_ascii=False)

        general_cols = [desc[0] for desc in cursor.description]
        remision_general = dict(zip(general_cols, general))

        # === Paso 2: Buscar la carga específica dentro de esa remisión ===
        cursor.execute("""
            SELECT uuid, carga, cantidad_solicitada, fecha_creacion
            FROM remisiones_cabecera
            WHERE id_remision_general = ?
            AND carga = ?
            AND cantidad_solicitada = ?
            AND DATE(fecha_creacion) = DATE(?)
            AND borrado = 0
            LIMIT 1
        """, (
            remision_general["uuid"],
            carga,
            float(cantidad_solicitada) if cantidad_solicitada else 0,
            today_date_str
        ))

        carga_row = cursor.fetchone()
        if not carga_row:
            conn.close()
            return json.dumps({}, ensure_ascii=False)

        carga_cols = [desc[0] for desc in cursor.description]
        carga_dict = dict(zip(carga_cols, carga_row))

        # === Paso 3: Obtener los detalles (tinas) de esa carga ===
        cursor.execute("""
            SELECT uuid AS cuerpo_id, sku_tina, sku_talla, tara, peso_neto, merma,
                lote, tanque, peso_marbete, peso_bascula,
                peso_neto_devolucion, peso_bruto_devolucion, observaciones, is_msc, is_sensorial
            FROM remisiones_cuerpo
            WHERE id_remision = ? AND borrado = 0
            ORDER BY fecha_creacion
        """, (carga_dict["uuid"],))

        detalles = cursor.fetchall()
        if detalles:
            detalles_cols = [d[0] for d in cursor.description]
            carga_dict["detalles"] = [dict(zip(detalles_cols, d)) for d in detalles]
        else:
            carga_dict["detalles"] = []

        # === Paso 4: Integrar todo en una estructura unificada ===
        remision_general["carga"] = carga_dict

        conn.close()
        return json.dumps(remision_general, ensure_ascii=False)


    def total_neto_entregado_por_id_remision_general(self, id_remision_general: str):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # === 1) Total peso_neto en remisiones_cuerpo ===
        cursor.execute("""
            SELECT COALESCE(SUM(rc.peso_neto), 0)
            FROM remisiones_cuerpo rc
            INNER JOIN remisiones_cabecera rch
                ON rc.id_remision = rch.uuid
            WHERE rch.id_remision_general = ?
            AND rc.borrado = 0
            AND rch.borrado = 0
        """, (id_remision_general,))
        total_cuerpo = cursor.fetchone()[0]

        # === 2) Total peso_neto en retallados ===
        cursor.execute("""
            SELECT COALESCE(SUM(peso_neto), 0)
            FROM remisiones_retallados
            WHERE id_remision_general = ?
            AND borrado = 0
        """, (id_remision_general,))
        total_retallados = cursor.fetchone()[0]

        conn.close()

        # === 3) Sumar ambos ===
        return total_cuerpo - total_retallados

    def todas_las_remisiones(self):
        """
        Devuelve todas las remisiones generales con sus cargas y detalles.
        """
        self._asegurar_tablas_remisiones()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # === Paso 1: Obtener todas las remisiones generales ===
        cursor.execute("""
            SELECT uuid, folio, cliente, numero_sello, placas_contenedor, fecha_produccion, factura, observaciones, fecha_creacion
            FROM remisiones_general WHERE borrado = 0
            ORDER BY fecha_creacion DESC
        """)
        generales = cursor.fetchall()

        if not generales:
            conn.close()
            return []

        generales_cols = [desc[0] for desc in cursor.description]
        remisiones = []

        # === Paso 2: Recorrer cada remisión general ===
        for general_row in generales:
            general_dict = dict(zip(generales_cols, general_row))

            # Obtener cargas asociadas
            cursor.execute("""
                SELECT uuid, carga, cantidad_solicitada, fecha_creacion
                FROM remisiones_cabecera
                WHERE id_remision_general = ? AND borrado = 0
                ORDER BY fecha_creacion
            """, (general_dict["uuid"],))
            cargas = cursor.fetchall()

            cargas_cols = [desc[0] for desc in cursor.description]
            cargas_list = []

            # === Paso 3: Recorrer cada carga y obtener sus detalles ===
            for carga_row in cargas:
                carga_dict = dict(zip(cargas_cols, carga_row))

                cursor.execute("""
                    SELECT uuid AS cuerpo_id, sku_tina, sku_talla, tara, peso_neto, merma,
                        lote, tanque, peso_marbete, peso_bascula,
                        peso_neto_devolucion, peso_bruto_devolucion, observaciones, is_msc, is_sensorial
                    FROM remisiones_cuerpo
                    WHERE id_remision = ? AND borrado = 0
                    ORDER BY fecha_creacion
                """, (carga_dict["uuid"],))
                detalles = cursor.fetchall()

                if detalles:
                    detalles_cols = [d[0] for d in cursor.description]
                    carga_dict["detalles"] = [dict(zip(detalles_cols, d)) for d in detalles]
                else:
                    carga_dict["detalles"] = []

                cargas_list.append(carga_dict)

            general_dict["cargas"] = cargas_list
            remisiones.append(general_dict)

        conn.close()
        return remisiones

    def obtener_remisiones_cuerpo_hoy(self):
        """
        Devuelve todas las tinas (remisiones_cuerpo) correspondientes
        a las remisiones generales creadas hoy.
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        today_date_str = datetime.now().strftime('%Y-%m-%d')

        cur.execute("""
            SELECT rc.*, 
                rch.carga, 
                rch.cantidad_solicitada,
                rg.folio, 
                rg.cliente, 
                rg.numero_sello, 
                rg.placas_contenedor, 
                rg.fecha_produccion, 
                rg.factura,
                rg.observaciones,
                rg.fecha_creacion AS fecha_remision
            FROM remisiones_cuerpo rc
            INNER JOIN remisiones_cabecera rch ON rc.id_remision = rch.uuid
            INNER JOIN remisiones_general rg ON rch.id_remision_general = rg.uuid
            WHERE DATE(rg.fecha_creacion) = ? AND rg.borrado = 0 AND rch.borrado = 0 AND rc.borrado = 0
            ORDER BY rg.fecha_creacion DESC, rch.carga, rc.fecha_creacion
        """, (today_date_str,))

        rows = [dict(row) for row in cur.fetchall()]
        conn.close()
        return rows

    def remisiones_por_rango(self, fecha_inicio: str, fecha_fin: str):
        """
        Devuelve todas las remisiones generales con sus cargas y detalles
        cuya fecha_creacion esté en [fecha_inicio, fecha_fin).
        """
        self._asegurar_tablas_remisiones()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # === Paso 1: Obtener remisiones generales dentro del rango ===
            cursor.execute("""
                SELECT uuid, folio, cliente, numero_sello, placas_contenedor, fecha_produccion, factura, observaciones, fecha_creacion
                FROM remisiones_general
                WHERE fecha_creacion >= ? AND fecha_creacion < ? AND borrado = 0
                ORDER BY fecha_creacion ASC
            """, (fecha_inicio, fecha_fin))
            generales = cursor.fetchall()

            if not generales:
                return []

            generales_cols = [desc[0] for desc in cursor.description]
            remisiones = []

            # === Paso 2: Recorrer cada remisión general ===
            for general_row in generales:
                general_dict = dict(zip(generales_cols, general_row))

                # === Paso 3: Obtener cargas asociadas ===
                cursor.execute("""
                    SELECT uuid, carga, cantidad_solicitada, fecha_creacion
                    FROM remisiones_cabecera
                    WHERE id_remision_general = ? AND borrado = 0
                    ORDER BY fecha_creacion
                """, (general_dict["uuid"],))
                cargas = cursor.fetchall()
                cargas_cols = [desc[0] for desc in cursor.description]

                cargas_list = []

                for carga_row in cargas:
                    carga_dict = dict(zip(cargas_cols, carga_row))

                    # === Paso 4: Obtener detalles asociados ===
                    cursor.execute("""
                        SELECT uuid AS cuerpo_id, sku_tina, sku_talla, tara, peso_neto, merma,
                            lote, tanque, peso_marbete, peso_bascula,
                            peso_neto_devolucion, peso_bruto_devolucion, observaciones, is_msc, is_sensorial
                        FROM remisiones_cuerpo
                        WHERE id_remision = ? AND borrado = 0
                        ORDER BY fecha_creacion
                    """, (carga_dict["uuid"],))
                    detalles = cursor.fetchall()

                    if detalles:
                        detalles_cols = [d[0] for d in cursor.description]
                        carga_dict["detalles"] = [dict(zip(detalles_cols, d)) for d in detalles]
                    else:
                        carga_dict["detalles"] = []

                    cargas_list.append(carga_dict)

                general_dict["cargas"] = cargas_list
                remisiones.append(general_dict)

            return remisiones

        finally:
            conn.close()

    def actualizar_campo_remision(self, tabla: str, id_local: str, campo: str, valor, id_remision_general=None):
        """
        Crea o actualiza un solo campo editable de un registro de remisiones.
        Si no existe el registro en la tabla indicada, lo crea automáticamente.
        """

        if tabla not in ["general", "cabecera", "cuerpo", "retallados"]:
            raise ValueError("Tabla inválida. Debe ser 'general', 'cabecera', 'cuerpo' o 'retallados'.")

        # === Configuración por tabla ===
        if tabla == "general":
            campos_editables = [
                "folio", "cliente", "numero_sello", "placas_contenedor", "fecha_produccion", "factura", "observaciones"
            ]
            tabla_sql = "remisiones_general"
            id_campo = "uuid"

        elif tabla == "cabecera":
            campos_editables = ["carga", "cantidad_solicitada"]
            tabla_sql = "remisiones_cabecera"
            id_campo = "uuid"

        elif tabla == "retallados":
            campos_editables = [
                "sku_tina", "sku_talla", "lote", "tara",
                "peso_bascula", "peso_neto", "observaciones"
            ]
            tabla_sql = "remisiones_retallados"
            id_campo = "uuid"
            
            # Para retallados, si el ID está vacío, generamos uno nuevo
            if not id_local or id_local.strip() in ('', 'undefined', 'null'):
                id_local = str(uuid.uuid4())

        else:  # cuerpo
            campos_editables = [
                "sku_tina", "sku_talla", "tara", "peso_neto", "merma", "lote",
                "tanque", "peso_marbete", "peso_bascula",
                "peso_neto_devolucion", "peso_bruto_devolucion",
                "observaciones", "is_msc", "is_sensorial"
            ]
            tabla_sql = "remisiones_cuerpo"
            id_campo = "uuid"

        if campo not in campos_editables:
            raise ValueError(f"Campo no editable: {campo}")

        # Convertir valores booleanos para checkboxes
        if campo in ['is_msc', 'is_sensorial']:
            valor_sql = 1 if valor in [True, 'true', '1', 1] else 0
        else:
            valor_sql = valor if valor not in (None, '') else None

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # === Verificar si existe el registro ===
            cursor.execute(f"SELECT COUNT(*) FROM {tabla_sql} WHERE {id_campo} = ? AND borrado = 0", (id_local,))
            existe = cursor.fetchone()[0] > 0

            if existe:
                # === Actualizar campo existente ===
                cursor.execute(
                    f"UPDATE {tabla_sql} SET {campo} = ? WHERE {id_campo} = ?",
                    (valor_sql, id_local)
                )
            else:
                # === Crear nuevo registro ===
                fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Columnas mínimas para cada tabla
                columnas = [id_campo, campo, "fecha_creacion"]
                valores = [id_local, valor_sql, fecha_actual]
                
                # Para retallados, agregar relación con la remisión general
                if tabla == "retallados" and id_remision_general:
                    columnas.append("id_remision_general")
                    valores.append(id_remision_general)

                placeholders = ", ".join(["?"] * len(columnas))
                columnas_sql = ", ".join(columnas)

                cursor.execute(
                    f"INSERT INTO {tabla_sql} ({columnas_sql}) VALUES ({placeholders})",
                    valores
                )

            conn.commit()
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

        return id_local

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
                ORDER BY cf.fecha_hora_guardado DESC
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

    def relacion_sku_descripcion(self) -> dict:
        """
        Devuelve un diccionario con todos los SKU y sus descripciones.
        Usa la misma lógica de descripcion_talla():
        - Si el SKU empieza con 'P' → especie + talla
        - Si el SKU empieza con 'B' o cualquier otro → descripción
        """
        self._asegurar_tabla_catalogo_de_talla()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        resultado = {}

        # Obtener todos los registros
        cursor.execute("SELECT sku, especie, talla, descripcion FROM catalogo_de_talla")
        filas = cursor.fetchall()

        for sku, especie, talla, descripcion in filas:
            if sku is None:
                continue

            if sku.startswith(('P', 'p')):
                desc = f"{descripcion or ''} {talla or ''} KG".strip()
            else:
                desc = descripcion or ""

            resultado[sku] = desc

        conn.close()
        return resultado

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
                    WHEN lote_fda LIKE 'P%' THEN
                        CASE
                            WHEN instr(lote_fda, 'L') > 0 THEN substr(lote_fda, 1, instr(lote_fda, 'L') - 1)
                            ELSE substr(lote_fda, 1, 10)
                        END
                    ELSE substr(lote_fda, 1, 6)
                END AS lote,
                SUM(peso_neto) AS total_peso_neto,
                COUNT(*) AS total_tinas
            FROM camaras_frigorifico
            WHERE estado = 0
            GROUP BY lote
        """)
        totales_por_lote = [
            {"lote": row[0], "total_peso_neto": row[1], "total_tinas": row[2]} for row in cursor.fetchall()
        ]

        # ---- Tabla por día + lote + talla ----
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN lote_fda LIKE 'P%' THEN
                        CASE
                            WHEN instr(lote_fda, 'L') > 0 THEN substr(lote_fda, 1, instr(lote_fda, 'L') - 1)
                            ELSE substr(lote_fda, 1, 10)
                        END
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
                    WHEN lote_fda LIKE 'P%' THEN
                        CASE
                            WHEN instr(lote_fda, 'L') > 0 THEN substr(lote_fda, 1, instr(lote_fda, 'L') - 1)
                            ELSE substr(lote_fda, 1, 10)
                        END
                    ELSE substr(lote_fda, 1, 6)
                END = (
                    SELECT 
                        CASE 
                            WHEN lote_fda LIKE 'P%' THEN
                                CASE
                                    WHEN instr(lote_fda, 'L') > 0 THEN substr(lote_fda, 1, instr(lote_fda, 'L') - 1)
                                    ELSE substr(lote_fda, 1, 10)
                                END
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

    def guardar_cambio_peso(self, campo, valor_anterior, nuevo_valor, uuid_remision):
        """Guarda un cambio en la tabla de historial."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO historial_peso (uuid_remision, campo_modificado, valor_anterior, nuevo_valor)
            VALUES (?, ?, ?, ?)
        ''', (uuid_remision, campo, valor_anterior, nuevo_valor))
        conn.commit()
        conn.close()

    def obtener_retallados(self, id_remision_general: str):
        """
        Devuelve todos los registros de retallados asociados a una remisión específica.
        
        :param id_remision_general: UUID de la remisión padre (general)
        :return: Lista de diccionarios con los campos de remisiones_retallados
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT *
            FROM remisiones_retallados
            WHERE id_remision_general = ? AND borrado = 0
            ORDER BY fecha_creacion ASC
        """, (id_remision_general,))

        filas = cursor.fetchall()
        conn.close()

        # Convertir cada fila en un diccionario
        return [dict(fila) for fila in filas]
    
    #esta funcion la usa el excel service
    def retallados_del_dia(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        fecha_local = datetime.now(ZoneInfo("America/Mexico_City")).strftime("%Y-%m-%d")
        cursor.execute("""
            SELECT sku_tina, sku_talla, lote, tara, peso_bascula, peso_neto, observaciones
            FROM remisiones_retallados
            WHERE DATE(fecha_creacion) = ? AND borrado = 0
            ORDER BY fecha_creacion ASC
            """, (fecha_local,))
        filas = cursor.fetchall()
        conn.close()
        return filas

    def eliminar_registro_remision(self, tabla, uuid_registro):
        """
        Elimina un registro por UUID en la tabla especificada.
        Guarda el evento en la tabla 'historial_peso' si aplica.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            if tabla not in ('retallados', 'cuerpo', 'cabecera', 'general'):
                raise ValueError(f"Tabla no permitida: {tabla}")
            tabla = "remisiones_" + tabla
            cursor.execute(f"SELECT * FROM {tabla} WHERE uuid = ?", (uuid_registro,))
            registro = cursor.fetchone()

            if registro:
                cursor.execute(f"UPDATE {tabla} SET borrado = 1 WHERE uuid = ?", (uuid_registro,))
                conn.commit()

        finally:
            conn.close()
