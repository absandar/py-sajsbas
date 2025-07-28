import sqlite3
from typing import Dict
from .interfaces import AlmacenamientoBase
from config import Config

DB_PATH = Config.DB_PATH

class SQLiteService(AlmacenamientoBase):
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

    def guardar(self, datos: Dict[str, str]):
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
                datos.get('fecha_hora_guardado'),
            ))
            conn.commit()
            conn.close()
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
