' Lanza snapshot_cierre.bat sin mostrar ventana de consola.
' 0 = ventana oculta, False = no esperar a que termine.
CreateObject("WScript.Shell").Run "cmd /c """ & CreateObject("Scripting.FileSystemObject").GetParentFolderName(WScript.ScriptFullName) & "\snapshot_cierre.bat""", 0, False
