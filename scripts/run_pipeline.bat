@echo off
REM Wrapper de pipeline.py para Task Scheduler:
REM - corre el pipeline completo
REM - dispara auto_commit_push al terminar
REM Pensado para schedule cada 4h en el servidor.

cd /d "%~dp0\.."

echo ==== %DATE% %TIME% PIPELINE START ==== >> "data\pipeline.log"
python "scripts\pipeline.py" >> "data\pipeline.log" 2>&1
echo ==== %DATE% %TIME% PIPELINE END (exit %ERRORLEVEL%) ==== >> "data\pipeline.log"

call "%~dp0auto_commit_push.bat" "auto: pipeline run"
