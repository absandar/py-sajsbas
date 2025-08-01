from serial.tools import list_ports

for port in list_ports.comports():
    print(f"Dispositivo: {port.device}")
    print(f"  Descripción: {port.description}")
    print(f"  Número de serie: {port.serial_number}")
    print(f"  VID: {port.vid}, PID: {port.pid}")
    print("-" * 40)