@echo off
cd /d "%~dp0"
docker compose up -d --build
if errorlevel 1 (
    echo.
    echo No se pudo iniciar Apache Big Data Lab. Revise los errores anteriores.
) else (
    echo.
    echo Para ver el sitio, haga clic en el siguiente enlace:
    echo http://localhost:8501
)
echo.
cmd /k
