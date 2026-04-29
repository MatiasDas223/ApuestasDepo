@echo off
REM Helper: stage + commit (si hay cambios) + pull --rebase + push.
REM Uso: auto_commit_push.bat "mensaje de commit"
REM Si no hay cambios, sale sin tocar el remote.

cd /d "%~dp0\.."

set "MSG=%~1"
if "%MSG%"=="" set "MSG=auto: scheduled commit"

echo ==== %DATE% %TIME% %MSG% ==== >> "data\auto_commit.log"

git add -A >> "data\auto_commit.log" 2>&1

git diff --staged --quiet
if not errorlevel 1 (
    echo Nothing to commit, skipping push >> "data\auto_commit.log"
    exit /b 0
)

git commit -m "%MSG% [%DATE% %TIME%]" >> "data\auto_commit.log" 2>&1

git pull --rebase >> "data\auto_commit.log" 2>&1
if errorlevel 1 (
    echo PULL --REBASE FAILED, abortando push >> "data\auto_commit.log"
    git rebase --abort >> "data\auto_commit.log" 2>&1
    exit /b 1
)

git push >> "data\auto_commit.log" 2>&1
if errorlevel 1 (
    echo PUSH FAILED >> "data\auto_commit.log"
    exit /b 1
)

echo OK >> "data\auto_commit.log"
exit /b 0
