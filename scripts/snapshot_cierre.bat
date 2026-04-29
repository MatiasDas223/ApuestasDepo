@echo off
REM Snapshot closing line + CLV.
REM Pensado para schedule cada 5 min en el servidor.
REM Al terminar dispara auto_commit_push (no hace push si no hay cambios).

cd /d "%~dp0\.."
echo ==== %DATE% %TIME% ==== >> "data\snapshot_cierre.log"
python "scripts\snapshot_cierre.py" --window 20 >> "data\snapshot_cierre.log" 2>&1

call "%~dp0auto_commit_push.bat" "auto: snapshot cierre"
