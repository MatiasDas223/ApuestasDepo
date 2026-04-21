@echo off
cd /d "C:\Users\Matt\Apuestas Deportivas"
echo ==== %DATE% %TIME% ==== >> "data\snapshot_cierre.log"
python "scripts\snapshot_cierre.py" --window 20 >> "data\snapshot_cierre.log" 2>&1
