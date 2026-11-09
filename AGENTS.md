# Organización técnica del proyecto

Apache Big Data Lab - TP2 de Procesamiento de Datos  
Nicolás Druetta - 2026

## 1. Organización del código

Separé la aplicación en módulos para que cada archivo tenga una tarea concreta. La interfaz no realiza directamente la descarga ni el procesamiento de los datos.

**Interfaz y funciones comunes**

- `main.py`: inicia Streamlit y organiza las secciones de la pantalla.
- `app/renders.py`: muestra los controles, las seis consolas, los gráficos, las preguntas y contiene componentes visuales reutilizables.
- `app/functions.py`: reúne funciones comunes de archivos, rutas y logs.
- `app/database.py`: contiene la conexión a PostgreSQL, las consultas y las operaciones SQL.

**Procesamiento**

- `pipeline/01_python_controller.py`: administra el RUN y coordina las etapas.
- `pipeline/02_alpaca_stream.py`: consulta y reproduce datos de Alpaca.
- `pipeline/03_apache_kafka.py`: publica los eventos en Kafka.
- `pipeline/04_apache_spark.py`: consume Kafka, procesa microbatches y escribe Parquet.
- `pipeline/05_pandas.py`: calcula métricas y correlaciones.
- `pipeline/06_postgresql.py`: coordina la persistencia de los resultados.

**Otros archivos**

- `assets/custom.css`: estilos y animaciones de la interfaz.
- `Dockerfile`: construcción de la imagen de la aplicación.
- `compose.yaml` y `compose.override.yaml`: servicios, puertos y volúmenes.
- `requirements.txt`: dependencias Python.
- `EJECUTAR en Windows.bat` y `EJECUTAR en Mac.command`: inicio desde Windows o macOS.

## 2. Recorrido de los datos

El Controller inicia una ejecución con un identificador propio (`run_id`). Alpaca descarga operaciones y cotizaciones históricas y las emite progresivamente. Kafka publica los mensajes y Spark los procesa mediante Structured Streaming.

Spark separa operaciones (`TRADE`) y cotizaciones (`QUOTE`), genera resultados por símbolo y escribe fragmentos Parquet. Pandas lee los fragmentos nuevos y calcula métricas. PostgreSQL guarda los resultados y Streamlit los consulta para mostrarlos.

El sistema combina reproducción histórica, streaming por microbatches y análisis de resultados. Que se utilice Structured Streaming no significa que se estén recibiendo cotizaciones en vivo.

## 3. Ejecuciones y archivos

Cada ejecución tiene un `run_id` con formato `YYYYMMDD_HHMMSS`. Ese identificador se utiliza en los logs, los archivos de resultados y las consultas de PostgreSQL.

Los registros de las seis etapas se guardan en archivos TXT separados. El Controller también genera información estructurada en JSONL. Spark trabaja con fragmentos Parquet y checkpoints y puede consolidar el resultado al finalizar.

La carpeta `data/` se genera automáticamente y se usa para archivos temporales de trabajo. No se distribuye con el RAR y se puede eliminar cuando los servicios están detenidos. La información de PostgreSQL se guarda por separado en su volumen Docker.

## 4. Inicio y detención

El botón de inicio prepara un RUN y pone en marcha las etapas correspondientes. El Controller mantiene el estado de la ejecución y el seguimiento de actividad.

El botón de detención solicita cortar la descarga, la publicación y el procesamiento del RUN. La detención tiene prioridad sobre completar los lotes pendientes. Por ese motivo, una ejecución interrumpida puede no tener un Parquet final consolidado.

Los errores se registran en los logs de las etapas que corresponden. El estado visual de Streamlit no reemplaza la información registrada sobre el procesamiento.

## 5. Kafka y Spark

Los mensajes están asociados a un `run_id` para evitar mezclar ejecuciones. Spark utiliza una referencia temporal de inicio con un margen de cinco segundos y filtra los eventos por RUN.

Spark Structured Streaming procesa microbatches y utiliza checkpoints. Si el análisis o la persistencia de un lote falla, la consulta debe informar el error en lugar de dar el lote por completado. Las escrituras en PostgreSQL utilizan UPSERT para evitar registros duplicados cuando se repite una operación.

La configuración de los servicios y los recursos asignados al clúster se encuentra en los archivos de Docker Compose.

## 6. Pandas y PostgreSQL

Pandas lee los fragmentos nuevos de Parquet durante el procesamiento normal. Cuando se reconstruye el estado de una ejecución, puede necesitar leer los fragmentos que ya existen.

Las métricas por símbolo se almacenan en `market_metrics`. La base también contiene `market_answers`, utilizada para los resultados numéricos de las preguntas de investigación. Las explicaciones visibles se presentan en la interfaz y no deben confundirse con los valores numéricos guardados.

Las tablas se crean cuando corresponde mediante operaciones idempotentes. No es necesario recrearlas en cada microbatch.

## 7. Criterios de programación

El proyecto sigue estas convenciones:

- Identificadores Python en inglés; funciones y variables en `snake_case`.
- Constantes en `UPPER_SNAKE_CASE` y clases en `PascalCase`.
- Indentación de cuatro espacios y formato general PEP 8.
- Imports organizados y funciones ubicadas en el módulo que corresponde.
- Comentarios y docstrings en español.
- Funciones comunes en `app/functions.py`, sin duplicarlas en cada etapa.
- Consultas y conexiones SQL en `app/database.py`.
- Interfaz en `main.py` y `app/renders.py`, sin incluir Streamlit en los módulos de procesamiento.
- Estilos personalizados en `assets/custom.css`.

Estas decisiones permiten encontrar más fácilmente cada responsabilidad y mantener el proyecto sin mezclar la interfaz con el procesamiento.

## 8. Ejecución en otra computadora

La instalación se explica en `README.md`. El proyecto se entrega sin `data/` y sin entorno virtual. Con Docker Desktop iniciado, el archivo de ejecución de Windows o macOS prepara los contenedores y permite acceder a la aplicación desde el navegador.

Para revisar problemas de una ejecución conviene consultar el RUN correspondiente y los logs de las seis etapas. Los archivos intermedios no indican, por sí solos, que un RUN haya terminado correctamente.
