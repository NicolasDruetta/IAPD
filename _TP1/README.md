# Trabajo Práctico 1 — Procesamiento de Datos

## Análisis de índices bursátiles internacionales

Proyecto desarrollado en Python que implementa un proceso ETL utilizando la arquitectura Medallion (Bronze, Silver y Gold).

Procesa datos históricos de 18 índices bursátiles internacionales obtenidos de Yahoo Finance, correspondientes al período 2006–2026, y realiza un análisis financiero sobre los años completos 2007–2025.

## Estructura del proyecto

- `main.py`: ejecución principal del proceso ETL.
- `extract/`: lectura de los datos originales (Bronze).
- `transform/`: transformación, depuración y validación (Silver).
- `load/`: cálculo de indicadores y generación de gráficos (Gold).
- `app/`: constantes y funciones compartidas.
- `data/`: dataset histórico en formato CSV.
- `outputs/`: seis gráficos generados en formato PNG.
- `etl_notebook.ipynb`: notebook complementario del análisis.
- `requirements.txt`: dependencias del proyecto.

## Instalación

Requiere Python 3.15.

**1. Crear un entorno virtual**

```bash
python -m venv .venv
```

**2. Activar el entorno virtual**

macOS / Linux:

```bash
source .venv/bin/activate
```

Windows:

```powershell
.venv\Scripts\activate
```

**3. Instalar las dependencias**

```bash
python -m pip install -r requirements.txt
```

## Ejecución

Desde la carpeta principal del proyecto:

```bash
python main.py
```

La aplicación ejecuta secuencialmente las tres capas ETL y genera seis gráficos PNG en `outputs/`, sobrescribiendo los archivos existentes.

Para exportar los gráficos se utiliza Kaleido, que requiere disponer de Chrome compatible.

## Notebook complementario

Para abrir el notebook:

```bash
python -m notebook
```

Seleccionar `etl_notebook.ipynb` desde Jupyter.

El notebook y la aplicación modular pueden ejecutarse de manera independiente.

## Datos

El dataset está incluido en `data/indices_bursatiles.csv`, por lo que no es necesario descargar datos durante la ejecución.

## Informe técnico

El informe PDF que acompaña la entrega explica la arquitectura, las transformaciones realizadas, los indicadores y la interpretación de los seis análisis bursátiles.