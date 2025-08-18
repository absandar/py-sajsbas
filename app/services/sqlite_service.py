import sqlite3
from typing import Dict
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                estado INTEGER DEFAULT 0
            )
        ''')
        conn.commit()
        conn.close()

    def guardar(self, datos: Dict[str, str]) -> int:
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            nueva_tara_val = datos.get('nueva_tara')
            if nueva_tara_val is not None and str(nueva_tara_val).strip() != '':
                tara_val = float(nueva_tara_val)
            else:
                tara_val = float(datos.get('tara', 0))
            
            cursor.execute('''
                INSERT INTO camaras_frigorifico (
                    id_procesa_app, fecha_de_descarga, certificado, sku_tina,
                    sku_talla, peso_bruto, tanque, hora_de_marbete,
                    hora_de_pesado, fda, lote_fda, lote_sap, peso_neto, tara, observaciones, fecha_hora_guardado
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
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
                datos.get('fecha_hora_guardado')
            ))
            last_id = cursor.lastrowid
            assert last_id is not None  
            conn.commit()
            conn.close()
            return last_id
        except Exception as e:
            print("Error guardando en SQLite:", e)
            raise

    def marcar_como_borrado(self, id: int):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("UPDATE camaras_frigorifico SET estado = 1 WHERE id_procesa_app = ?", (id,))
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


    def guardar_catalogo_de_tina(self, datos: list[Dict[str, str]]):
        self._asegurar_tabla_catalogo_de_tina()
        """Vacía e inserta nuevos registros en catalogo_de_tina."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Vaciar tabla antes de insertar
            cursor.execute("DELETE FROM catalogo_de_tina")

            # Insertar datos
            for item in datos:
                cursor.execute(
                    "INSERT INTO catalogo_de_tina (sku, tara) VALUES (?, ?)",
                    (item['sku'], int(item['tara']))
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

            # Insertar datos
            for item in datos:
                cursor.execute(
                    "INSERT INTO catalogo_de_talla (sku, descripcion, especie, talla) VALUES (?, ?, ?, ?)",
                    (
                        item.get('sku', ''),
                        item.get('descripcion', ''),
                        item.get('especie', ''),
                        item.get('talla', '')
                    )
                )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            return False

    def fusionar_con_local(self, datos: list[dict]):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for item in datos:
            uuid = item.get('id')  # o mejor usar un UUID real si lo tienes
            cursor.execute("SELECT COUNT(*) FROM camaras_frigorifico WHERE id_procesa_app = ?", (uuid,))
            existe = cursor.fetchone()[0]

            if not existe:
                cursor.execute("""
                    INSERT INTO camaras_frigorifico (
                        id_procesa_app, fecha_de_descarga, certificado, sku_tina,
                        sku_talla, peso_bruto, tanque, hora_de_marbete,
                        hora_de_pesado, fda, lote_fda, lote_sap, peso_neto, tara,
                        observaciones, fecha_hora_guardado, estado
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item.get('id'),
                    item.get('fecha_de_descarga'),
                    item.get('certificado'),
                    item.get('sku_tina'),
                    item.get('sku_talla'),
                    float(item.get('peso_bruto', 0)),
                    item.get('tanque'),
                    item.get('hora_de_marbete'),
                    item.get('hora_de_pesado'),
                    item.get('fda'),
                    item.get('lote_fda'),
                    item.get('lote_sap'),
                    float(item.get('peso_neto', 0)),
                    float(item.get('tara', 0)),
                    item.get('observaciones'),
                    item.get('fecha_hora_guardado'),
                    item.get('estado')
                ))

        conn.commit()
        conn.close()

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

    def guardar_catalogo_barcos(self, datos: list[dict]):
        """Vacía e inserta nuevos registros en catalogo_de_barcos."""
        self._asegurar_tabla_catalogo_de_barcos()
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("DELETE FROM catalogo_de_barcos")

            for item in datos:
                cursor.execute("""
                    INSERT INTO catalogo_de_barcos (
                        inicial, descripcion
                    ) VALUES (?, ?)
                """, (
                    item.get('inicial', ''),
                    item.get('descripcion', ''),
                ))

            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"Error al guardar catalogo_de_barcos: {e}")
            log_error(f"Error al guardar el sqlite: {e}")
            return False

    def _asegurar_tabla_cola_sincronizacion(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cola_sincronizacion (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tabla TEXT NOT NULL,
                id_registro INTEGER NOT NULL,
                tipo_operacion TEXT NOT NULL,
                fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                procesado INTEGER DEFAULT 0
            );
        ''')
        conn.commit()
        conn.close()

    def agregar_a_cola(self, tabla: str, id_registro: int, tipo_operacion: str):
        self._asegurar_tabla_cola_sincronizacion()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO cola_sincronizacion (tabla, id_registro, tipo_operacion)
            VALUES (?, ?, ?)
        """, (tabla, id_registro, tipo_operacion))
        conn.commit()
        conn.close()

    def obtener_pendientes_cola(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM cola_sincronizacion WHERE procesado = 0
        """)
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def marcar_como_procesado(self, id_cola: int):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE cola_sincronizacion SET procesado = 1 WHERE id = ?
        """, (id_cola,))
        conn.commit()
        conn.close()

    def obtener_registro(self, tabla: str, id_registro: int) -> dict:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {tabla} WHERE id = ?", (id_registro,))
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else {}
    
    def actualizar_id_nube(self, tabla: str, id_registro: int, id_procesa_app: int):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            f"UPDATE {tabla} SET id_procesa_app = ? WHERE id = ?",
            (id_procesa_app, id_registro)
        )
        conn.commit()
        conn.close()

    def actualizar_campo(self, id_local: int, campo: str, valor):
        """Actualiza un solo campo editable de un registro local."""
        try:
            # solo permite campos que son editables
            campos_editables = ['sku_tina', 'sku_talla', 'peso_bruto', 'tara', 'tanque']
            if campo not in campos_editables:
                raise ValueError(f"Campo no editable: {campo}")

            # Si el valor es None o vacío, usar NULL
            valor_sql = valor if valor not in (None, '') else None

            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(f"UPDATE camaras_frigorifico SET {campo} = ? WHERE id = ?", (valor_sql, id_local))
            conn.commit()
            conn.close()

        except Exception as e:
            log_error(f"Error actualizando campo {campo} del registro {id_local}: {e}")
            raise

    def obtener_id_nube(self, id_local: int) -> int:
        """Obtiene el id_procesa_app del registro local."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT id_procesa_app FROM camaras_frigorifico WHERE id = ?", (id_local,))
            resultado = cursor.fetchone()
            conn.close()
            # Devuelve 0 si no existe id_procesa_app
            return int(resultado[0]) if resultado and resultado[0] not in (None, "", "0") else 0
        except Exception as e:
            log_error(f"Error obteniendo id_procesa_app del registro {id_local}: {e}")
            return 0

    def obtener_registro_por_id(self, id_local: int) -> dict:
        """Obtiene todos los campos del registro como diccionario."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM camaras_frigorifico WHERE id = ?", (id_local,))
            row = cursor.fetchone()
            columnas = [col[0] for col in cursor.description]
            conn.close()
            if row:
                return dict(zip(columnas, row))
            else:
                raise Exception(f"Registro {id_local} no encontrado")
        except Exception as e:
            log_error(f"Error obteniendo registro {id_local}: {e}")
            raise
    
    def migraciones(self):
        queries = [
            # Ejemplos de migraciones
            "ALTER TABLE camaras_frigorifico ADD COLUMN isSyncro INTEGER DEFAULT 0",
            "ALTER TABLE camaras_frigorifico DROP COLUMN isSyncro",
            "ALTER TABLE camaras_frigorifico ADD COLUMN isSyncFromCloud INTEGER DEFAULT 0",
            "ALTER TABLE camaras_frigorifico ADD COLUMN isSyncToCloud INTEGER DEFAULT 0",
            "ALTER TABLE camaras_frigorifico DROP COLUMN isSyncFromCloud",
            "ALTER TABLE camaras_frigorifico DROP COLUMN isSyncToCloud",
        ]

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        resultados = []
        for i, query in enumerate(queries, start=1):
            try:
                cursor.execute(query)
                resultados.append(f"Query {i} ejecutada correctamente")
            except sqlite3.OperationalError as e:
                resultados.append(f"Error ejecutando query {i}: {e}")
        conn.commit()
        conn.close()
        return "<br>".join(resultados)

    def obtener_ultimos_13(self):
        """Devuelve los últimos 13 registros activos (estado=0)."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id, id_procesa_app, sku_tina, sku_talla, tara, peso_bruto, peso_neto, 
                    lote_fda, fecha_hora_guardado, tanque
                FROM camaras_frigorifico
                WHERE estado = 0
                ORDER BY id_procesa_app DESC
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
        cursor.execute("SELECT descripcion FROM catalogo_de_barcos WHERE inicial = ?", (letra))
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