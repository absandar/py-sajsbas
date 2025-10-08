from datetime import datetime
import json
import os, sys
from openpyxl import Workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.styles import NamedStyle
from openpyxl.cell.rich_text import TextBlock, CellRichText
from openpyxl.cell.text import InlineFont
from openpyxl.drawing.image import Image
from openpyxl.utils import range_boundaries
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from app.services.sqlite_service import SQLiteService

ruta_actual = os.path.dirname(os.path.abspath(__file__))
image_path = os.path.join(ruta_actual, "images", "logo_procesa.png")

sqlservice = SQLiteService()
#recibo un return json.dumps(list(data.values()), ensure_ascii=False)
cargas_de_dia = sqlservice.cargas_del_dia()
# Crear libro nuevo
wb: Workbook = Workbook()
ws: Worksheet = wb.active  # type: ignore[assignment]

# Escribir valor en una celda específica
ws["B2"] = "Hola mundo"
ws.sheet_view.zoomScale = 55
COLOR_GRIS = PatternFill(start_color='E7E6E6', end_color='E7E6E6', fill_type='solid')
thin_border = Border(
    left=Side(style='thin'),
    right=Side(style='thin'),
    top=Side(style='thin'),
    bottom=Side(style='thin')
)
rangos = [
    'A1:C4',
    'D1:N4',
    'O1:Q4',
    'A6:B6',
    'D6:E6',
    'F6:Q6',
    'A7:B7',
    'D7:E7',
    'F7:H7',
    'I7:J7',
    'K7:M7',
    'O7:Q7'
]
for rango in rangos:
    ws.merge_cells(rango)
    min_col, min_row, max_col, max_row = range_boundaries(rango)
    # Dibujar solo el borde exterior del bloque fusionado
    for row in range(min_row, max_row + 1):
        for col in range(min_col, max_col + 1):
            cell = ws.cell(row=row, column=col)
            left   = 'thin' if col == min_col else None
            right  = 'thin' if col == max_col else None
            top    = 'thin' if row == min_row else None
            bottom = 'thin' if row == max_row else None
            cell.border = Border(
                left=Side(style=left),
                right=Side(style=right),
                top=Side(style=top),
                bottom=Side(style=bottom)
            )

ws['C6'].border = thin_border
ws['C7'].border = thin_border
ws['N7'].border = thin_border

anchos = {
    'A': 8.67, 'B': 33.78, 'C': 17.56, 'D': 47.33, 'E': 22.22, 'F': 16,
    'G': 14, 'H': 13.78, 'I': 14, 'J': 12.33, 'K': 16.56, 'L': 17.44,
    'M': 21, 'N': 20, 'O': 26.67, 'P': 13.33, 'Q': 13.33
}
for col, width in anchos.items():
    ws.column_dimensions[col].width = width

# === Alturas de fila ===
alturas = {
    1: 39, 2: 39, 3: 39, 4: 39,
    5: 4.2, 6: 27, 7: 27, 8: 4.2, 9: 63
}
for fila, altura in alturas.items():
    ws.row_dimensions[fila].height = altura
# === Aplicar estilo Calibri 16 negrita a A6:Q7 ===
for row in ws['A6:Q7']:
    for cell in row:
        cell.font = Font(name='Calibri', size=16, bold=True)
        cell.alignment = Alignment(horizontal='center', vertical='center')

for row in ws['A9:Q9']:
    for cell in row:
        cell.font = Font(name='Calibri', size=16, bold=True)
        cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
        cell.fill = COLOR_GRIS
        cell.border = thin_border
        
# definicion celdas grises
ws['A6'].fill = COLOR_GRIS
ws['D6'].fill = COLOR_GRIS
ws['A7'].fill = COLOR_GRIS
ws['D7'].fill = COLOR_GRIS
ws['I7'].fill = COLOR_GRIS
ws['N7'].fill = COLOR_GRIS

# agregar imagenes
if not os.path.exists(image_path):
    raise FileNotFoundError(f"No se encontró la imagen en: {image_path}")
img = Image(image_path)
ws.add_image(img, 'A1')


#textos fijos
ws['D1'] = "ALMACÉN - CÁMARAS FRIGORÍFICAS\nREMISIÓN DE ATÚN FRESCO CONGELADO"
ws['D1'].font = Font(name='Calibri', size=26, bold=True)
ws['D1'].alignment = Alignment(
    horizontal='center',
    vertical='center',
    wrap_text=True
)
ws['A6'] = "Folio"
ws['A7'] = "Número de sello"
ws['D6'] = "Cliente"
ws['D7'] = "Placas de contenedor"
ws['I7'] = "Factura / Pedido"
ws['N7'] = "Fecha"
rich_text = CellRichText()
rich_text.append(TextBlock(InlineFont(rFont='Calibri', sz=22), "Código: "))
rich_text.append(TextBlock(InlineFont(rFont='Calibri', sz=22, b=True), "F-AL-02\n"))
rich_text.append(TextBlock(InlineFont(rFont='Calibri', sz=22), "Fecha: 29-09-2025\n"))
rich_text.append(TextBlock(InlineFont(rFont='Calibri', sz=22), "Página: 1 de 1\n"))
rich_text.append(TextBlock(InlineFont(rFont='Calibri', sz=22), "Revisión: 01\n"))
rich_text.append(TextBlock(InlineFont(rFont='Calibri', sz=22), "Referenciado a M-AL-01"))

ws['O1'] = rich_text
ws['O1'].alignment = Alignment(
    horizontal='left',
    vertical='center',
    wrap_text=True
)
ws['A9'] = "N°"
ws['B9'] = "Carga"
ws['C9'] = "SKU"
ws['D9'] = "Descripción"
ws['E9'] = "Lote"
ws['F9'] = "Tanque"
ws['G9'] = "Tina"
ws['H9'] = "Tara"
ws['I9'] = "Peso Bruto"
ws['J9'] = "Peso Salida"
ws['K9'] = "Merma"
ws['L9'] = "Peso Entrada"
ws['M9'] = "Peso Neto Devolución"
ws['N9'] = "Peso Bruto Devolución"
ws['O9'] = "Observaciones"
ws['P9'] = "MSC"
ws['Q9'] = "Evaluación sensorial"

try:
    remision_general = json.loads(cargas_de_dia)
except Exception as e:
    print(f"Error al decodificar cargas_del_dia: {e}")
    remision_general = {}

if not remision_general or "cargas" not in remision_general:
    print("No hay datos de remisiones.")
else:
    # === Encabezado general ===
    ws['C6'] = remision_general.get("folio", "")
    ws['C7'] = remision_general.get("numero_sello", "")
    ws['F6'] = remision_general.get("cliente", "")
    ws['F7'] = remision_general.get("placas_contenedor", "")
    ws['K7'] = remision_general.get("factura", "")
    fecha = remision_general.get("fecha_creacion", "")
    ws['O7'] = datetime.strptime(fecha, "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y") if fecha else ""

    fila_inicial = 9  # empieza debajo de encabezados (A9:Q9)
    cargas = remision_general.get("cargas", [])

    contador = 0
    for idx_carga, carga in enumerate(cargas, start=1):
        for detalle in carga.get("detalles", []):
            contador = contador+1
            fila_inicial += 1
            ws[f"A{fila_inicial}"] = contador
            ws[f"B{fila_inicial}"] = "CARGA: " +carga.get("carga", "")
            ws[f"C{fila_inicial}"] = detalle.get("sku_talla", "")
            ws[f"E{fila_inicial}"] = detalle.get("lote", "")
            ws[f"F{fila_inicial}"] = detalle.get("tanque", "")
            ws[f"G{fila_inicial}"] = detalle.get("sku_tina", "")
            ws[f"H{fila_inicial}"] = detalle.get("tara", "")
            ws[f"I{fila_inicial}"] = detalle.get("peso_bascula", "")
            ws[f"J{fila_inicial}"] = detalle.get("peso_neto", "")
            ws[f"K{fila_inicial}"] = detalle.get("merma", "")

            total_peso_neto += float(detalle.get("peso_neto") or 0)
            total_merma += float(detalle.get("merma") or 0)
            total_peso_marbete += float(detalle.get("peso_marbete") or 0)
            total_peso_neto_devol += float(detalle.get("peso_neto_devolucion") or 0)

            ws[f"L{fila_inicial}"] = detalle.get("peso_marbete", "")
            ws[f"M{fila_inicial}"] = (
                "" if not detalle.get("peso_neto_devolucion") or detalle.get("peso_neto_devolucion") == 0 
                else detalle.get("peso_neto_devolucion")
            )
            ws[f"M{fila_inicial}"] = (
                "" if not detalle.get("peso_bruto_devolucion") or detalle.get("peso_bruto_devolucion") == 0 
                else detalle.get("peso_bruto_devolucion")
            )
            ws[f"O{fila_inicial}"] = detalle.get("observaciones", "")
            ws.row_dimensions[fila_inicial].height = 21.6
            # aplicar bordes y formato
            for col in range(1, 18):  # columnas A–Q
                c = ws.cell(row=fila_inicial, column=col)
                c.border = thin_border
                c.alignment = Alignment(horizontal='center', vertical='center',wrapText=True)
                c.font = Font(name='Calibri', size=12)

    # === Aplicar bordes al bloque general ===
    for row in ws.iter_rows(min_row=10, max_row=fila_inicial, min_col=1, max_col=17):
        for cell in row:
            cell.border = thin_border


wb.save("ejemplo.xlsx")
