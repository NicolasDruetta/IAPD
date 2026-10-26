# Apache Big Data Lab

## 1. Presentación

**Materia:** Procesamiento de Datos  
**Trabajo:** Trabajo Práctico N.º 2  
**Autor:** Nicolás Druetta

Apache Big Data Lab es un proyecto académico para estudiar el procesamiento distribuido y el análisis de datos financieros mediante tecnologías de Big Data. La aplicación aborda el recorrido completo de los datos: ingesta, procesamiento, almacenamiento, análisis y visualización.

La propuesta utiliza información del mercado estadounidense y relaciona cada componente tecnológico con conceptos de paralelismo, distribución, streaming, persistencia y análisis de datos.

## 2. Objetivos

### Objetivo general

Diseñar y documentar una plataforma modular de procesamiento distribuido aplicada al análisis de datos financieros.

### Objetivos específicos

- Analizar el flujo de datos financieros desde la ingesta hasta la visualización.
- Experimentar con procesamiento paralelo y distribuido.
- Integrar eventos en tiempo real y datos históricos reproducibles.
- Aplicar transformaciones, agregaciones y consultas sobre grandes volúmenes de registros.
- Estudiar el procesamiento batch y el procesamiento de streaming.
- Analizar el almacenamiento distribuido y el formato columnar Apache Parquet.
- Observar particiones, tareas, tiempos de ejecución y métricas de rendimiento.
- Aplicar técnicas de análisis y Machine Learning sobre datos financieros.
- Documentar resultados, limitaciones y condiciones de ejecución.

## 3. Tecnologías

### Tecnologías implementadas

- **Python:** lenguaje principal.
- **Streamlit:** interfaz de la aplicación.
- **Alpaca Market Data:** conexión WebSocket para recibir eventos financieros.

La implementación disponible permite gestionar una conexión de streaming, suscribirse a símbolos financieros y visualizar los eventos recibidos en la interfaz.

La conexión WebSocket utiliza el almacén de certificados de confianza proporcionado por `certifi`, manteniendo la verificación del certificado del servidor y del nombre de host.

### Tecnologías previstas

- **Apache Kafka:** ingesta, tópicos, particiones y distribución de eventos.
- **Apache Hadoop HDFS:** almacenamiento distribuido de datos y resultados.
- **Apache Parquet:** persistencia columnar eficiente.
- **Apache Spark y PySpark:** procesamiento distribuido, RDD, DataFrames y tareas paralelas.
- **Apache Spark SQL:** consultas y análisis mediante SQL.
- **Apache Structured Streaming:** microbatches, ventanas y agregaciones continuas.
- **Apache Flink:** procesamiento alternativo de flujos de eventos.
- **Apache Airflow:** orquestación de tareas y procesos batch.
- **Apache Spark MLlib:** preparación, entrenamiento y evaluación de modelos.

Las tecnologías previstas no se consideran implementadas hasta contar con una integración funcional y verificable.

## 4. Arquitectura general

El flujo conceptual del proyecto es:

```text
Alpaca Market Data
        ↓
Apache Kafka
        ↓
Apache Spark / Structured Streaming
        ↓
Apache Hadoop HDFS / Apache Parquet
        ↓
Análisis y visualización mediante Streamlit
```

Apache Flink se contempla como motor alternativo para el procesamiento de eventos. Apache Airflow se contempla como componente de orquestación para tareas batch, validaciones y generación de resultados.

La arquitectura mantiene separadas la interfaz, la lógica específica de cada tecnología y la persistencia de los datos.

## 5. Organización del proyecto

```text
NicolasDruetta_TP2/
├── main.py
├── README.md
├── requirements.txt
├── AGENTS.md
├── pages/
│   └── 01_alpaca_streaming.py
├── data/
├── outputs/
└── assets/
    └── custom.css
```

- `main.py`: interfaz principal de Streamlit y funciones compartidas.
- `pages/`: funciones específicas de cada tecnología o funcionalidad.
- `pages/01_alpaca_streaming.py`: conexión, autenticación, suscripción y recepción de eventos de Alpaca Market Data.
- `data/`: archivos de datos.
- `outputs/`: resultados generados.
- `assets/`: recursos visuales.
- `requirements.txt`: dependencias del proyecto.
- `AGENTS.md`: convenciones técnicas de desarrollo.

## 6. Funcionalidades

El proyecto contempla las siguientes funcionalidades:

- Streaming financiero en modalidades LIVE y REPLAY.
- Ingesta de eventos mediante Apache Kafka.
- Almacenamiento distribuido mediante Hadoop HDFS.
- Persistencia en formato Apache Parquet.
- Procesamiento distribuido con Apache Spark y PySpark.
- Consultas mediante Apache Spark SQL.
- Procesamiento continuo con Apache Structured Streaming.
- Procesamiento alternativo mediante Apache Flink.
- Orquestación con Apache Airflow.
- Machine Learning con Apache Spark MLlib.
- Benchmarking de tiempos, velocidad, speedup y eficiencia.
- Comparación opcional con Python tradicional.

## 7. Ejecución

La ejecución requiere Python 3.15, disponible en Windows y macOS, y las dependencias declaradas en `requirements.txt`.

La instalación se realiza con:

```bash
python -m pip install -r requirements.txt
```

La aplicación se inicia mediante Streamlit:

```bash
python -m streamlit run main.py
```

La conexión segura con Alpaca utiliza `certifi`, incluido en las dependencias del proyecto. No requiere certificados instalados manualmente ni rutas específicas del sistema operativo.

## 8. Convenciones de desarrollo

Las convenciones técnicas del proyecto están documentadas en [`AGENTS.md`](AGENTS.md). Allí se establecen los criterios de estilo Python, nomenclatura, organización de funciones, separación entre interfaz y procesamiento, documentación y uso de dependencias.

## 9. Estado del proyecto

### Implementado

- Estructura base de la aplicación Streamlit.
- Interfaz principal en `main.py`.
- Integración de streaming WebSocket con Alpaca Market Data.
- Suscripción a símbolos financieros.
- Recepción y visualización acotada de eventos.
- Inicio y detención de la conexión desde la interfaz.

### Previsto

- Integración de Apache Kafka, Hadoop HDFS, Apache Spark, Spark SQL, Structured Streaming, Flink, Airflow y Spark MLlib.
- Persistencia distribuida y análisis de datos históricos.
- Métricas de rendimiento, benchmarking y comparación con Python tradicional.
