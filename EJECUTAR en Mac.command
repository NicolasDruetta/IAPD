#!/bin/bash
cd "$(dirname "$0")" || exit 1
if ! docker info >/dev/null 2>&1; then
    echo ""
    echo "Docker Desktop no se encuentra abierto. Abra Docker Desktop y ejecute nuevamente este archivo."
elif docker compose up -d --build; then
    echo ""
    echo "Para ver el sitio, haga clic en el siguiente enlace:"
    echo "http://localhost:8502"
else
    echo ""
    echo "No se pudo iniciar Apache Big Data Lab. Revise los errores anteriores."
fi
echo ""
exec "${SHELL:-/bin/zsh}" -i
