import tkinter as tk
from tkinter import messagebox
import subprocess
import webbrowser
import os
import signal
import sys
import ctypes

myappid = "miempresa.miapp.control_app12891823212"  # usa un ID único
ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)

# Ruta al script que inicia waitress
SCRIPT_PATH = os.path.abspath("run.py")
SERVER_URL = "http://localhost:8088"

server_process = None

def actualizar_estado(texto):
    estado_var.set(f"Estado: {texto}")

def iniciar_servidor():
    global server_process
    if server_process is None:
        try:
            server_process = subprocess.Popen([sys.executable, SCRIPT_PATH])
            webbrowser.open(SERVER_URL)
            actualizar_estado("Ejecutando")
            messagebox.showinfo("Servidor", "Aplicacion iniciada.")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo iniciar:\n{e}")
    else:
        messagebox.showinfo("Servidor", "Ya está en ejecución.")

def detener_servidor():
    global server_process
    if server_process:
        try:
            os.kill(server_process.pid, signal.SIGTERM)
            server_process = None
            actualizar_estado("Detenido")
            messagebox.showinfo("Servidor", "Aplicacion detenida.")
        except Exception as e:
            messagebox.showerror("Error", f"No se pudo detener:\n{e}")
    else:
        messagebox.showinfo("Servidor", "No está en ejecución.")

def reiniciar_servidor():
    detener_servidor()
    root.after(1000, iniciar_servidor)

# GUI
root = tk.Tk()
root.title("Control App Camaras Frick")
root.geometry("300x200")
root.resizable(False, False)

# Establecer el ícono de la ventana
icon_path = os.path.abspath("icono.ico")
if os.path.exists(icon_path):
    root.iconbitmap(icon_path)

frame = tk.Frame(root, padx=20, pady=20)
frame.pack()

tk.Button(frame, text="Iniciar Aplicacion", width=25, command=iniciar_servidor).pack(pady=5)
tk.Button(frame, text="Detener Aplicacion", width=25, command=detener_servidor).pack(pady=5)
tk.Button(frame, text="Reiniciar Aplicacion", width=25, command=reiniciar_servidor).pack(pady=5)

# Estado
estado_var = tk.StringVar()
estado_var.set("Estado: Detenido")
estado_label = tk.Label(root, textvariable=estado_var, font=("Arial", 10), fg="blue")
estado_label.pack(pady=5)

root.mainloop()
