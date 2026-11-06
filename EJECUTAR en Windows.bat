@echo off
cd /d "%~dp0"
docker info >nul 2>&1
if errorlevel 1 (
    echo.
    echo Docker Desktop no se encuentra abierto. Abra Docker Desktop y ejecute nuevamente este archivo.
) else (
    docker compose up -d --build
    if errorlevel 1 (
    echo.
    echo No se pudo iniciar Apache Big Data Lab. Revise los errores anteriores.
    ) else (
        echo.
        echo Para ver el sitio, haga clic en el siguiente enlace:
        echo http://localhost:8502
    )
)
echo.
cmd /k
