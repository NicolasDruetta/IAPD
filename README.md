# Apache Big Data Lab

## 1. Presentación

**Materia:** Procesamiento de Datos  
**Trabajo:** Trabajo Práctico N.º 2  
**Autor:** Nicolás Druetta

Apache Big Data Lab es un proyecto académico orientado al procesamiento de
datos financieros mediante herramientas de Big Data. La aplicación integra
una interfaz Streamlit con Alpaca Market Data para obtener operaciones reales y
con Apache Spark distribuido dentro de Docker.

El sistema permite observar el ciclo de obtención, persistencia, recuperación
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
- **Streamlit:** interfaz de usuario y actualización periódica de la
  aplicación.
- **Alpaca Market Data:** estado del mercado, operaciones LIVE y datos
  históricos REPLAY.
- **WebSocket:** recepción de operaciones durante el modo LIVE.
- **JSONL:** persistencia incremental de eventos y registros de ejecución.
- **certifi:** almacén de certificados utilizado para conexiones seguras.
- **Apache Spark y PySpark:** procesamiento distribuido mediante un Driver,
  un Master y dos Workers dentro de Docker.
- **Docker Compose:** orquestación reproducible de la aplicación y del clúster.

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

## 4. Arquitectura del proyecto

El flujo conceptual considerado por el proyecto es:

```text
Navegador
    ↓
Streamlit + Alpaca + Spark Driver
    ↓
Spark Master
    ├── Spark Worker 1
    └── Spark Worker 2

Alpaca Market Data → Persistencia JSONL → Lotes PySpark → Streamlit
```

Kafka, Hadoop HDFS, Parquet, Flink y Airflow forman parte de la arquitectura
académica prevista, pero todavía no están integrados.

## 5. Procesamiento de datos

La carpeta `data/` no se distribuye mediante Git y no contiene un dataset
obligatorio para ejecutar el proyecto.

Los datos se obtienen directamente desde Alpaca Market Data:

- **LIVE:** recibe operaciones bursátiles reales mediante WebSocket cuando el
  mercado presenta actividad.
- **REPLAY:** consulta la API histórica de Alpaca, descarga operaciones reales
  y las reproduce progresivamente cuando corresponde utilizar datos
  históricos.

Al presionar «Iniciar streaming», la aplicación consulta el estado oficial del
mercado proporcionado por Alpaca. Si informa que el mercado está abierto, se
inicia LIVE mediante WebSocket. Si informa que está cerrado, se inicia REPLAY.
La decisión contempla fines de semana, feriados y cierres especiales mediante
el estado operativo de Alpaca; no depende de la ausencia temporal de
operaciones recibidas.

El modo seleccionado queda asociado al RUN y no cambia mientras la ejecución
está activa. Al refrescar Streamlit, la aplicación recupera el RUN existente y
no vuelve a consultar el mercado ni crea otra conexión.

REPLAY no depende de archivos históricos incluidos en el repositorio. Los
históricos descargados se almacenan en `data/historical/` y se reutilizan como
caché local para evitar descargas innecesarias. Cada ejecución genera sus
propios registros en `data/streaming/`. Estos archivos son datos generados por
la aplicación y no deben versionarse.

## 6. Organización del proyecto

```text
NicolasDruetta_TP2/
├── main.py
├── README.md
├── requirements.txt
├── AGENTS.md
├── compose.yaml
├── .dockerignore
├── docker/
│   └── Dockerfile
├── pages/
│   ├── 01_alpaca_streaming.py
│   └── 04_apache_spark.py
├── data/
│   ├── historical/
│   └── streaming/
└── assets/
    └── custom.css
```

- `main.py`: punto de entrada de Streamlit, controles, tarjetas de estado,
  consolas y funciones comunes.
- `pages/01_alpaca_streaming.py`: conexión LIVE, descarga histórica,
  reproducción REPLAY, ciclo de vida y persistencia de Alpaca.
- `pages/04_apache_spark.py`: procesamiento específico de Spark distribuido.
- `compose.yaml`: servicios Streamlit/Driver, Master y dos Workers.
- `docker/Dockerfile`: imagen común con Python, PySpark y Java.
- `.dockerignore`: archivos excluidos del contexto de construcción.
- `data/historical/`: caché local de históricos descargados desde Alpaca.
- `data/streaming/`: metadata y eventos generados por cada ejecución.
- `assets/custom.css`: recurso visual centralizado del proyecto.
- `requirements.txt`: dependencias Python declaradas.
- `AGENTS.md`: convenciones técnicas de desarrollo.

La carpeta `data/` se genera o completa localmente y permanece excluida del
control de versiones.

## 7. Interfaz de usuario

La interfaz presenta un encabezado con el nombre del laboratorio, la
descripción del trabajo y los datos académicos del autor. Debajo se ubican el
selector de símbolos y los controles de ejecución. La organización mantiene
una distribución responsive para evitar que los textos y botones se compriman
en pantallas pequeñas.

Los símbolos predeterminados son `AAPL, MSFT, NVDA, SPY, TSLA, META, SPCX,
GLD, BTC, OXY`. El campo se deshabilita durante una ejecución activa, un
inicio pendiente o una detención pendiente.

La fila de controles utiliza una distribución de seis unidades para símbolos y
seis unidades para acciones. Dentro del área de acciones, iniciar, detener y
el espacio de reserva se distribuyen en proporciones `2, 2, 2`. Las tarjetas
de estado se presentan en el orden `RUN`, `Modo`, `Eventos recibidos` y
`Estado`, con proporciones `2, 2, 2, 6`.

El botón de iniciar se identifica visualmente con color verde y un icono de
play. El botón de detener utiliza color rojo y un icono de stop. Durante las
transiciones se muestran los estados `Iniciando...`, `Ejecutando streaming...`
y `Deteniendo...` junto con un indicador de carga. El botón detener solo se
presenta cuando existe una ejecución activa o una detención pendiente.

La tarjeta de estado informa el estado actual, el tiempo transcurrido y, cuando
corresponde, la fecha y hora de la última ejecución. El RUN identifica la
ejecución activa o la última ejecución disponible.

La interfaz presenta tres consolas:

1. **Log de ejecución:** ciclo de vida, conexión, descarga, workers, heartbeat
   y cierre.
2. **Alpaca Streaming:** operaciones actuales en LIVE u operaciones históricas
   en REPLAY.
3. **Apache Spark:** estado y resultados del procesamiento Spark disponible.

Las consolas se actualizan junto con el estado del RUN y mantienen una
presentación uniforme. En pantallas reducidas, las columnas se reorganizan
mediante media queries clásicas para conservar la legibilidad.

## 8. Apache Spark

El módulo de Spark utiliza un Driver dentro del contenedor de Streamlit y se
conecta al Master mediante `spark://spark-master:7077`. El clúster está
compuesto por dos Workers.

El procesamiento realiza lo siguiente:

- Reutiliza una única `SparkSession` distribuida.
- Procesa un lote acotado de operaciones.
- Crea un DataFrame con esquema explícito.
- Reparte el lote por símbolo.
- Agrupa las operaciones por símbolo.
- Calcula cantidad de operaciones y volumen total.
- Informa particiones y duración mediante `time.perf_counter()`.

Los lotes se procesan sin crear un job por cada evento individual. Los
resultados reales se muestran en la consola Spark.

## 9. Entorno de ejecución

El único requisito del anfitrión es Docker Desktop, disponible para macOS
Apple Silicon, macOS Intel y Windows x86-64. Python, PySpark y Java se
instalan dentro de la imagen del proyecto.

Desde la raíz del repositorio se ejecuta:

```bash
docker compose up --build
```

La aplicación queda disponible en:

- Streamlit: `http://localhost:8501`
- Spark Master UI: `http://localhost:8080`
- Spark Worker 1 UI: `http://localhost:8081`
- Spark Worker 2 UI: `http://localhost:8082`

Para detener los servicios:

```bash
docker compose down
```

Los datos se conservan en el volumen Docker `lab-data`, montado en `/app/data`.
No se incorporan datasets históricos al repositorio.

## 10. Estado del proyecto

### Implementado

- Aplicación Streamlit con `main.py` como punto de entrada.
- Integración LIVE y REPLAY con Alpaca Market Data.
- Consulta del estado oficial del mercado para seleccionar el modo.
- Descarga automática y caché local de datos históricos.
- Persistencia incremental de ejecuciones y eventos en JSONL.
- Logs de ejecución, consola de operaciones y consola Spark.
- Imagen Docker reproducible con Python 3.15, PySpark 4.2.0 y Java 17.
- Compose con Streamlit/Driver, Master y dos Workers.
- Configuración de Spark distribuido mediante `spark://spark-master:7077`.
- Volumen persistente para `data/`.

### Pendiente de verificación en Docker

- Registro efectivo de ambos Workers en el Master.
- Creación de `SparkSession` dentro del contenedor Streamlit.
- Ejecución distribuida de DataFrame, Jobs, Stages y Tasks.
- Confirmación de que ambos Workers ejecutan tareas reales.

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

## 11. Convenciones de desarrollo

Las convenciones de programación, organización del código, nomenclatura,
imports, separación de líneas, comentarios y uso de decoradores están
documentadas en [`AGENTS.md`](AGENTS.md).
