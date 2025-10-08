@echo off
::cd /d C:\ruta\de\tu\proyecto

:: Ejecutar en modo producción
start /b python run.py production

:: Espera 3 segundos
timeout /t 3 /nobreak > nul

:: Abre el navegador con la URL
start "" "http://127.0.0.1:8088"

pause
