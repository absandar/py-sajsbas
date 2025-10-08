# Ruta al archivo objetivo (.bat, .exe, .py, etc.)
$TargetPath = "$env:APPDATA\recepcion_pescado\RecepcionPescado.bat"

# Carpeta donde se guardará inicialmente el acceso directo
$ShortcutOutputFolder = "$env:TEMP"

# Nombre del acceso directo
$ShortcutName = "Recepcion Pescado.lnk"

# Ruta completa del acceso directo
$ShortcutFullPath = Join-Path -Path $ShortcutOutputFolder -ChildPath $ShortcutName

# Crear acceso directo
$WshShell = New-Object -ComObject WScript.Shell
$Shortcut = $WshShell.CreateShortcut($ShortcutFullPath)
$Shortcut.TargetPath = $TargetPath
$Shortcut.WorkingDirectory = Split-Path -Path $TargetPath
$Shortcut.IconLocation = "$env:SystemRoot\System32\shell32.dll,1"
$Shortcut.Save()

Write-Host "Acceso directo creado en: $ShortcutFullPath"
