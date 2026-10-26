# Apache Big Data Lab

## 1. Presentación

**Materia:** Procesamiento de Datos  
**Trabajo:** Trabajo Práctico N.º 2  
**Autor:** Nicolás Druetta

Apache Big Data Lab es un proyecto académico orientado al procesamiento de
datos financieros mediante herramientas de Big Data. La aplicación integra una
interfaz Streamlit con la obtención de operaciones bursátiles reales desde
Alpaca Market Data y una integración inicial con Apache Spark para el
procesamiento local de lotes acotados.

El proyecto permite observar el ciclo de obtención, persistencia, recuperación
y visualización de datos financieros, manteniendo separadas la interfaz y la
lógica específica de cada tecnología.

## 2. Objetivos

### Objetivo general

Analizar un flujo de datos financieros reales y preparar su procesamiento
mediante herramientas de paralelismo, streaming y Big Data.

### Objetivos específicos

- Obtener operaciones bursátiles reales mediante una fuente externa.
- Procesar datos en tiempo real y mediante reproducción histórica.
- Persistir los eventos recibidos en formato JSONL.
- Aplicar procesamiento local mediante PySpark sobre lotes acotados.
- Observar cantidades de registros, particiones y duración de las operaciones.
- Mantener una interfaz clara para visualizar el estado y los resultados.
- Establecer una base modular para las tecnologías previstas del trabajo.

## 3. Tecnologías

### Implementadas

- **Python 3.15:** lenguaje principal del proyecto.
- **Streamlit:** interfaz de usuario y actualización periódica de la aplicación.
- **Alpaca Market Data:** estado de mercado, operaciones LIVE y datos históricos
  REPLAY.
- **WebSocket:** recepción de operaciones durante el modo LIVE.
- **JSONL:** persistencia incremental de eventos y registros de ejecución.
- **certifi:** almacén de certificados utilizado para las conexiones seguras.
- **Apache Spark y PySpark:** código de procesamiento local preparado para
  crear DataFrames y agrupar operaciones por símbolo.

La integración de Spark es parcial. El código está incorporado, pero la
ejecución actual no está disponible porque el entorno no cuenta con Java. Por
ese motivo, todavía no se consideran verificadas la creación de una
`SparkSession`, la ejecución de DataFrames ni la obtención de resultados Spark.

### Previstas

- **Apache Kafka:** ingesta y distribución de eventos mediante tópicos.
- **Apache Hadoop HDFS:** almacenamiento distribuido.
- **Apache Parquet:** persistencia columnar.
- **Apache Spark SQL:** consultas y análisis mediante SQL.
- **Apache Structured Streaming:** procesamiento continuo por microbatches.
- **Apache Flink:** procesamiento alternativo de flujos.
- **Apache Airflow:** orquestación de tareas y procesos batch.
- **Apache Spark MLlib:** análisis y modelos de Machine Learning.

Las tecnologías previstas no forman parte de la ejecución funcional actual.

## 4. Arquitectura general

El flujo conceptual considerado por el proyecto es:

```text
Alpaca Market Data
        ↓
Obtención LIVE o descarga histórica REPLAY
        ↓
Persistencia local JSONL
        ↓
Procesamiento local mediante Apache Spark
        ↓
Visualización mediante Streamlit
```

Kafka, Hadoop HDFS, Parquet, Flink y Airflow forman parte de la arquitectura
académica prevista, pero todavía no están integrados en la aplicación actual.

## 5. Origen y almacenamiento de datos

La carpeta `data/` no se distribuye mediante Git y no contiene un dataset
obligatorio para ejecutar el proyecto.

Los datos se obtienen directamente desde Alpaca Market Data:

- **LIVE:** recibe operaciones bursátiles reales mediante WebSocket cuando el
  mercado presenta actividad.
- **REPLAY:** consulta la API histórica de Alpaca, descarga operaciones reales
  y las reproduce progresivamente cuando corresponde utilizar datos históricos.

Al presionar «Iniciar streaming», la aplicación consulta el estado de mercado
proporcionado por Alpaca. Si Alpaca informa que el mercado está abierto, se
inicia LIVE mediante WebSocket. Si informa que está cerrado, se inicia REPLAY.
La decisión contempla fines de semana, feriados y cierres especiales mediante
el estado operativo de Alpaca; no depende de la ausencia temporal de
operaciones recibidas.

El modo seleccionado queda asociado al RUN y no cambia mientras la ejecución
está activa. Al refrescar Streamlit, la aplicación recupera el RUN existente y
no vuelve a consultar el mercado ni crea otra conexión.

REPLAY no depende de archivos históricos incluidos en el repositorio.

Los históricos descargados se almacenan en `data/historical/` y se reutilizan
como caché local para evitar descargas innecesarias. Cada ejecución genera sus
propios registros en `data/streaming/`. Estos archivos son datos generados por
la aplicación y no deben versionarse.

## 6. Organización del proyecto

```text
NicolasDruetta_TP2/
├── main.py
├── README.md
├── requirements.txt
├── AGENTS.md
├── pages/
│   ├── 01_alpaca_streaming.py
│   └── 04_apache_spark.py
├── data/
│   ├── historical/
│   └── streaming/
└── assets/
    └── custom.css
```

- `main.py`: punto de entrada de Streamlit, controles, métricas, consolas y
  funciones comunes.
- `pages/01_alpaca_streaming.py`: conexión LIVE, descarga histórica,
  reproducción REPLAY, ciclo de vida y persistencia de Alpaca.
- `pages/04_apache_spark.py`: procesamiento específico de Spark local.
- `data/historical/`: caché local de históricos descargados desde Alpaca.
- `data/streaming/`: metadata y eventos generados por cada ejecución.
- `assets/custom.css`: recurso visual centralizado del proyecto.
- `requirements.txt`: dependencias Python declaradas.
- `AGENTS.md`: convenciones técnicas de desarrollo.

La carpeta `data/` se genera o completa localmente y permanece excluida del
control de versiones.

## 7. Funcionalidades actuales

### Alpaca Streaming

- Recepción LIVE mediante WebSocket.
- Autenticación y suscripción a símbolos financieros.
- Selección automática entre LIVE y REPLAY mediante el estado de Alpaca.
- Descarga histórica paginada mediante la API de Alpaca.
- Caché local de históricos ya descargados.
- Reproducción progresiva de operaciones históricas.
- Persistencia incremental de eventos en JSONL.
- Identificación de cada ejecución mediante `run_id`.
- Registro visible del ciclo de vida, la conexión, la descarga, los workers y
  el cierre.
- Heartbeat de la sesión visual y cierre por inactividad.
- Recuperación del estado de la ejecución durante los refrescos de Streamlit
  dentro del mismo proceso.

### Interfaz Streamlit

La interfaz presenta tres consolas de igual ancho:

1. **Log de ejecución:** ciclo de vida, descarga, workers, heartbeat y cierre.
2. **Alpaca Streaming:** operaciones recibidas o reproducidas.
3. **Apache Spark:** estado y resultados del procesamiento Spark disponible.

Las consolas utilizan componentes nativos de Streamlit y se organizan en
columnas que pueden apilarse en pantallas pequeñas.

### Apache Spark

El módulo de Spark contiene una ejecución local preparada para:

- Reutilizar una única `SparkSession`.
- Procesar un lote acotado de operaciones.
- Crear un DataFrame con esquema explícito.
- Agrupar las operaciones por símbolo.
- Calcular cantidad de operaciones y volumen total.
- Informar particiones y duración mediante `time.perf_counter()`.

La ejecución real permanece pendiente porque falta Java en el entorno actual.
La interfaz informa esta indisponibilidad sin presentar workers, stages, tasks
ni resultados inventados.

## 8. Ejecución

El entorno virtual Python se prepara fuera del repositorio en cada computadora.
Luego se clona el repositorio y se instalan las dependencias declaradas:

```bash
python -m pip install -r requirements.txt
```

La aplicación se inicia con:

```bash
python -m streamlit run main.py
```

Para ejecutar el procesamiento Spark se requiere además una instalación de
Java compatible con PySpark y disponible para el entorno local. Java no se
instala mediante `requirements.txt` y su disponibilidad debe verificarse en la
computadora donde se ejecute el proyecto.

La aplicación obtiene los datos desde Alpaca y guarda localmente los históricos
y registros generados. No se requieren archivos históricos incluidos en Git.

## 9. Convenciones de desarrollo

Las convenciones de programación, organización del código, nomenclatura,
imports, separación de líneas, comentarios y uso de decoradores están
documentadas en [`AGENTS.md`](AGENTS.md).

## 10. Estado del proyecto

### Implementado

- Aplicación Streamlit con `main.py` como punto de entrada.
- Integración LIVE y REPLAY con Alpaca Market Data.
- Descarga automática y caché local de datos históricos.
- Persistencia incremental de ejecuciones y eventos en JSONL.
- Logs de ejecución, consola de operaciones y consola Spark.
- Selección automática del modo mediante el estado de mercado de Alpaca.
- Módulo de procesamiento Spark local preparado para lotes acotados.

### Parcialmente implementado

- Apache Spark está incorporado en el código y en la interfaz, pero no puede
  ejecutarse en el entorno actual porque falta Java.
- No se verificó todavía una ejecución exitosa de `SparkSession`, DataFrame y
  agregación.

### Previsto

- Apache Kafka.
- Apache Hadoop HDFS.
- Apache Parquet.
- Apache Spark SQL.
- Apache Structured Streaming.
- Apache Flink.
- Apache Airflow.
- Apache Spark MLlib.
- Procesamiento distribuido multinodo y benchmarking comparativo.
