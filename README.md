# Apache Big Data Lab

## 1. Presentación

**Materia:** Procesamiento de Datos  
**Trabajo:** Trabajo Práctico N.º 2  
**Autor:** Nicolás Druetta

Apache Big Data Lab es un proyecto académico orientado al procesamiento de
datos financieros mediante herramientas de Big Data. La aplicación integra
una interfaz Streamlit con Alpaca Market Data para obtener cotizaciones y
operaciones bursátiles reales y
con Apache Spark distribuido dentro de Docker.

El sistema permite observar el ciclo de obtención, persistencia, recuperación
y visualización de datos financieros, manteniendo separadas la interfaz y la
lógica específica de cada tecnología.

## 2. Objetivos

### Objetivo general

Analizar un flujo de datos financieros reales y preparar su procesamiento
mediante herramientas de paralelismo, streaming y Big Data.

### Objetivos específicos

- Obtener cotizaciones y operaciones bursátiles reales mediante una fuente externa.
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
- **Alpaca Market Data:** estado del mercado, cotizaciones y operaciones LIVE y
  datos históricos REPLAY.
- **WebSocket:** recepción de cotizaciones y operaciones durante el modo LIVE.
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
    Controller → Alpaca → Spark Driver
    ↓
Spark Master
    ├── Spark Worker 1
    └── Spark Worker 2

Alpaca Market Data → Persistencia JSONL → Controller → Lotes PySpark → Streamlit
```

Kafka, Hadoop HDFS, Parquet, Flink y Airflow forman parte de la arquitectura
académica prevista, pero todavía no están integrados.

## 5. Procesamiento de datos

La carpeta `data/` no se distribuye mediante Git y no contiene un dataset
obligatorio para ejecutar el proyecto.

Los datos se obtienen directamente desde Alpaca Market Data:

- **LIVE:** recibe cotizaciones y operaciones bursátiles reales mediante
  WebSocket cuando el mercado presenta actividad.
- **REPLAY:** consulta las APIs históricas de Alpaca, descarga cotizaciones y
  operaciones reales y las reproduce progresivamente cuando corresponde
  utilizar datos históricos.

Al presionar «Iniciar streaming», la aplicación consulta el estado oficial del
mercado proporcionado por Alpaca. Si informa que el mercado está abierto, se
inicia LIVE mediante WebSocket. Si informa que está cerrado, se inicia REPLAY.
La decisión contempla fines de semana, feriados y cierres especiales mediante
el estado operativo de Alpaca; no depende de la ausencia temporal de
  eventos bursátiles recibidos.

El modo seleccionado queda asociado al RUN y no cambia mientras la ejecución
está activa. Al refrescar Streamlit, la aplicación recupera el RUN existente y
no vuelve a consultar el mercado ni crea otra conexión.

REPLAY no depende de archivos históricos incluidos en el repositorio. Cada RUN
nuevo genera exactamente cinco archivos directamente en `data/`, usando la
hora de `America/Argentina/Cordoba` y el identificador `YYYYMMDD_HHMMSS`:

```text
data/
├── YYYYMMDD_HHMMSS_data_alpaca.jsonl
├── YYYYMMDD_HHMMSS_log_01_controller.txt
├── YYYYMMDD_HHMMSS_log_02_alpaca_stream.txt
├── YYYYMMDD_HHMMSS_log_03_apache_spark.txt
└── YYYYMMDD_HHMMSS_run_info.jsonl
```

`_run_info.jsonl` contiene solo información estructurada de control,
recuperación y checkpoints Spark. `_data_alpaca.jsonl` conserva las
cotizaciones y operaciones originales de Alpaca, identificadas como `QUOTE` o
`TRADE`. Cada TXT es la fuente única de verdad de su
consola: Python genera la línea numerada y la agrega una sola vez; la interfaz
lee ese TXT sin renumerar ni reformatear. Los RUN históricos conservan su
formato anterior y continúan siendo legibles.

Cuando el campo de símbolos está vacío, el RUN obtiene la composición actual
del S&P 500 desde su tabla pública de componentes:
`https://en.wikipedia.org/wiki/List_of_S%26P_500_companies`. La página se
consulta en cada nuevo RUN, se normalizan sus símbolos y se conservan todas las
clases de acciones incluidas en la tabla. La definición del índice se contrasta
con la referencia de S&P Dow Jones Indices:
`https://www.spglobal.com/spdji/en/indices/equity/sp-500/`. Cada componente se
valida contra el endpoint oficial de activos de Alpaca; solo se incorporan
activos estadounidenses activos y tradables que Alpaca devuelve. Si la fuente
o la validación falla, el RUN informa el error y no sustituye el universo por
todo el mercado.

El RUN conserva el universo resuelto, la fuente, la fecha informada por la
fuente, la fecha de consulta, los componentes encontrados, los símbolos
disponibles y los no disponibles. Por eso el listado no cambia durante la
ejecución. Los símbolos ingresados manualmente se mantienen sin esta
restricción y no se filtran al S&P 500.

REPLAY utiliza además la última sesión bursátil regular completa y disponible
según el calendario oficial de Alpaca. La fecha se selecciona a partir de las
sesiones informadas por ese calendario, por lo que contempla fines de semana,
feriados y cierres anticipados. La apertura y el cierre oficiales se
interpretan en `America/New_York` y se convierten a UTC antes de consultar la
API histórica. Por lo tanto, REPLAY se limita a la sesión regular de esa fecha
y no consulta las horas previas a la apertura.

La descarga se divide en ventanas temporales de 15 minutos, ajustadas a la
apertura y al cierre reales, y grupos de símbolos con un máximo de cuatro
solicitudes HTTP concurrentes. Cada solicitud respeta la paginación de Alpaca,
el límite de 10.000 registros y reintenta HTTP 429 con espera progresiva.

Cada símbolo de una ventana se persiste en un archivo temporal directamente en
`/tmp` del contenedor y luego se mezcla mediante un merge cronológico acotado.
Los nombres identifican el RUN, la fecha bursátil, el feed, la ventana, el
grupo y la posición del símbolo. Así las operaciones se emiten progresivamente
mientras continúa la descarga de ventanas posteriores. Los temporales se
eliminan al finalizar cada merge, al detenerse el RUN o ante un error, y nunca
aparecen dentro del volumen `lab-data`. Un histórico incompleto no se considera
caché válida.

Durante REPLAY, el log informa la selección de la sesión, el universo de
símbolos, el avance de grupos completados, las respuestas HTTP, las páginas
con operaciones, las operaciones descargadas, la consolidación de cada
ventana y los lotes emitidos. El progreso se actualiza por grupos y lotes,
sin registrar una línea por operación individual.

Los cinco archivos nuevos se escriben incrementalmente y en modo append-only,
sin truncamiento ni sobrescritura. La interfaz conserva un cursor de bytes por
consola, carga tras F5 únicamente las últimas 1.000 líneas del TXT y mantiene
el contador en el número de la última línea persistida. Las líneas anteriores
permanecen intactas y cada consola ofrece la descarga de su TXT completo.

Los históricos legacy existentes se conservan sin modificación. Cuando uno es
válido para una nueva sesión, se reutiliza como fuente mediante enlace físico
para crear el nombre histórico del RUN sin duplicar físicamente sus datos. Si
el enlace no está disponible, la caché no se reutiliza y se descarga el RUN,
evitando una duplicación silenciosa.

### Zona horaria de presentación

Toda la interfaz presenta fechas y horas en `America/Argentina/Cordoba`,
correspondiente a UTC−03:00. La visualización convierte los timestamps
zonificados a la hora argentina exclusivamente en la capa de presentación,
para que los logs, los eventos y los estados sean consistentes en cualquier
sistema operativo y entorno Docker.

Las consultas bursátiles, la selección de sesiones y el calendario oficial de
Alpaca continúan utilizando `America/New_York`, que es la referencia del
mercado estadounidense. Los timestamps originales recibidos de Alpaca se
conservan en UTC para la persistencia y el procesamiento. De este modo, la
aplicación convierte esos valores únicamente al mostrarlos, sin alterar el
orden cronológico, los datos JSONL ni los cálculos distribuidos.

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
│   ├── 01_controller.py
│   ├── 02_alpaca_stream.py
│   └── 03_apache_spark.py
├── data/
│   └── (archivos de RUN y directorios legacy conservados)
└── assets/
    └── custom.css
```

- `main.py`: punto de entrada de Streamlit, controles, tarjetas de estado,
  consolas y funciones comunes.
- `pages/01_controller.py`: ciclo de vida, estados y supervisión del RUN.
- `pages/02_alpaca_stream.py`: conexión LIVE, descarga histórica,
  reproducción REPLAY, ciclo de vida y persistencia de Alpaca.
- `pages/03_apache_spark.py`: procesamiento específico de Spark distribuido.
- `compose.yaml`: servicios Streamlit/Driver, Master y dos Workers.
- `docker/Dockerfile`: imagen común con Python, PySpark y Java.
- `.dockerignore`: archivos excluidos del contexto de construcción.
- `data/`: cinco archivos por cada RUN nuevo; los directorios legacy existentes
  se conservan para compatibilidad.
- `assets/custom.css`: recurso visual centralizado del proyecto.
- `requirements.txt`: dependencias Python declaradas.
- `AGENTS.md`: convenciones técnicas de desarrollo.

La carpeta `data/` se genera o completa localmente y permanece excluida del
control de versiones.

## 7. Interfaz de usuario

La interfaz presenta un encabezado con el nombre del laboratorio, el subtítulo
«Análisis de operaciones bursátiles con procesamiento distribuido de Apache
Spark.» y los datos académicos del autor. Debajo se ubican el
selector de símbolos y los controles de ejecución. La organización mantiene
una distribución responsive para evitar que los textos y botones se compriman
en pantallas pequeñas.

Cuando el campo está vacío, el universo predeterminado corresponde a los
componentes actuales del S&P 500 validados contra Alpaca. El campo se
deshabilita durante una ejecución activa, un inicio pendiente o una detención
pendiente.

El selector también ofrece `Top 20 más líquidas del S&P 500` y `Top 20 con más
volumen del S&P 500`. Estas opciones no contienen símbolos fijos: al iniciar
un RUN se resuelve la composición actual del S&P 500, se consultan en Alpaca
los snapshots de esos componentes y se ordenan por cantidad de operaciones
(`trades`) para liquidez o volumen (`volume`). Se ejecuta el RUN únicamente
con las 20 acciones resultantes de ese momento. La selección y los símbolos
resueltos quedan persistidos en la información estructurada del RUN.

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

1. **Log del controlador:** ciclo de vida, conexión, descarga, workers, heartbeat
   y cierre.
2. **Obtención de cotizaciones y operaciones bursátiles:** datos actuales en
   LIVE o datos históricos en REPLAY.
3. **Procesamiento distribuido de cotizaciones y operaciones bursátiles:**
   estado y resultados de Spark.

Las consolas se actualizan junto con el estado del RUN y mantienen una
presentación uniforme. En pantallas reducidas, las columnas se reorganizan
mediante media queries clásicas para conservar la legibilidad.

La actualización de las consolas utiliza un componente v2 registrado en línea
desde `main.py`, con HTML y JavaScript puros. Cada visor conserva su mismo
`textarea` y recibe solamente el delta posterior a su cursor de bytes dentro
del TXT correspondiente. JavaScript agrega las líneas sin calcular numeración
y mantiene como máximo las últimas 1.000 visibles.

El tamaño de cada transferencia es únicamente un lote técnico de transporte;
no representa un límite de historial, retención ni cantidad máxima de entradas.
Los archivos persistentes permanecen completos. Después de F5, cada consola
lee directamente las últimas 1.000 líneas y coloca el scroll al final, sin
recuperar progresivamente todo el historial. El gráfico
Top 10 continúa dentro del fragmento periódico y solo utiliza los resultados
Spark ya disponibles, sin ejecutar trabajos adicionales para la actualización
visual.

Debajo de las consolas se presenta el gráfico **Top 10 de acciones por volumen
negociado**. El volumen se calcula exclusivamente sumando el campo `size` de
las operaciones `TRADE` reales emitidas por Alpaca y confirmadas por Apache
Spark. Las cantidades `bid_size` y `ask_size` de las cotizaciones `QUOTE` no
forman parte de ese volumen. Cada
lote procesado actualiza un acumulado pequeño por símbolo, recalcula el ranking
y actualiza las diez barras ordenadas de forma descendente durante LIVE o
REPLAY, sin esperar la finalización del RUN.

El acumulado se asocia al `run_id` y se persiste mediante registros tipados
`spark_checkpoint` dentro de `_run_info.jsonl`. Spark procesa solo
los eventos posteriores al último checkpoint, por lo que una actualización de
Streamlit o un F5 no vuelve a contar lotes ya confirmados ni carga el histórico
completo para redibujar el gráfico.

## 8. Apache Spark

El módulo de Spark utiliza un Driver dentro del contenedor de Streamlit y se
conecta al Master mediante `spark://spark-master:7077`. El clúster está
compuesto por dos Workers.

El procesamiento realiza lo siguiente:

- Reutiliza una única `SparkSession` distribuida.
- Procesa todos los eventos disponibles del RUN, separando `TRADE` y `QUOTE`.
- Crea un DataFrame con esquema explícito.
- Reparte el lote por símbolo.
- Agrupa los eventos por símbolo y tipo.
- Calcula operaciones ejecutadas, cotizaciones recibidas y volumen negociado.
- Informa particiones y duración mediante `time.perf_counter()`.

Los lotes se procesan sin crear un job por cada evento individual. Los
resultados reales se muestran en la consola Spark junto con el número de lote,
los registros procesados, las particiones utilizadas, la duración y los
errores efectivos cuando corresponda.

## 9. Entorno de ejecución

El único requisito del anfitrión es Docker Desktop, disponible para macOS
Apple Silicon, macOS Intel y Windows x86-64. Python, PySpark y Java se
instalan dentro de la imagen del proyecto.

### Límites de recursos Docker

Los límites se aplican exclusivamente en `compose.yaml` y mantienen los cuatro
servicios activos:

| Servicio | CPU máxima | Memoria máxima |
| --- | ---: | ---: |
| Streamlit / Spark Driver | 1.5 | 2 GiB |
| Spark Master | 0.5 | 512 MiB |
| Spark Worker 1 | 1 | 1 GiB |
| Spark Worker 2 | 1 | 1 GiB |

Los Workers conservan un core cada uno y disponen de 768 MiB para la memoria
de Spark. El Driver utiliza 1 GiB de memoria de JVM y los Executors 512 MiB,
con 256 MiB de overhead. El límite total de los contenedores es de 4.5 GiB y
no requiere modificar Windows, WSL ni Docker Desktop.

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

### Desarrollo con actualización automática

Para desarrollar sin reconstruir la imagen en cada cambio, Docker Compose carga
automáticamente `compose.override.yaml`. Ese archivo monta el repositorio local
en `/app`, mantiene los datos exclusivamente en el volumen `lab-data` montado
en `/app/data` y configura Streamlit para detectar cambios mediante polling.
Este mecanismo funciona con volúmenes compartidos de Docker Desktop en macOS y
Windows.

Se inicia con:

```bash
docker compose up
```

Los cambios en `main.py`, `pages/`, `assets/` y los recursos del proyecto se
reflejan al guardar y actualizar el navegador. El Master y los Workers no se
reconstruyen ni se reinician por esos cambios. La imagen solo debe
reconstruirse cuando cambien `requirements.txt`, `docker/Dockerfile` u otra
configuración de dependencias:

```bash
docker compose up --build
```

## 10. Estado del proyecto

### Implementado

- Aplicación Streamlit con `main.py` como punto de entrada.
- Integración LIVE y REPLAY con Alpaca Market Data.
- Consulta del estado oficial del mercado para seleccionar el modo.
- Selección de la última sesión regular completa mediante el calendario oficial
  de Alpaca, con conversión explícita de `America/New_York` a UTC.
- Descarga automática y caché local de datos históricos.
- REPLAY incremental por ventanas temporales, con cuatro solicitudes HTTP
  concurrentes como máximo y reintentos acotados para HTTP 429.
- Emisión progresiva y merge cronológico global sin cargar el día completo en
  memoria.
- Persistencia incremental en cinco archivos por RUN, con TXT de consola
  numerados, recuperación por cursores y compatibilidad con RUN históricos.
- Gráfico incremental del Top 10 de volumen acumulado confirmado por Spark.
- Visualización de las últimas 1.000 entradas por consola, con historial
  completo conservado en los TXT persistentes.
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
