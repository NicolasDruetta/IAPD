# Apache Big Data Lab

**Autor:** Nicolás Druetta

## 1. Descripción

Desarrollé esta aplicación como un proyecto personal para procesar y analizar datos bursátiles de Estados Unidos mediante distintas herramientas. El programa descarga operaciones y cotizaciones históricas de Alpaca por ventanas, agrupa los eventos según su segundo bursátil original y entrega cada lote directamente a Kafka para procesarlo con Spark. Después calcula métricas con Pandas, las guarda en PostgreSQL y muestra los resultados en Streamlit.

Los datos son reales, pero la ejecución reproduce una sesión pasada. No es una conexión en vivo al mercado debido a las limitaciones del plan gratuito de Alpaca para recibir datos en tiempo real de múltiples acciones.

## 2. Objetivo

Mi objetivo es implementar un flujo completo de procesamiento de datos y poder seguir lo que ocurre en cada etapa. También busco responder dos preguntas a partir de los datos obtenidos:

1. ¿Las acciones con más operaciones presentan mayor volatilidad?
2. ¿Las acciones más negociadas tienen menor diferencia entre los precios de compra y venta (spread)?

Las respuestas se calculan con los datos de cada ejecución. Una correlación no demuestra una relación de causa y efecto.

## 3. Tecnologías

- **Python:** lenguaje principal del proyecto.
- **Streamlit:** interfaz web para iniciar y detener ejecuciones y consultar resultados.
- **Alpaca Market Data:** descarga de operaciones (`TRADE`) y cotizaciones (`QUOTE`).
- **Apache Kafka:** publicación de los eventos en un tópico.
- **Apache Spark:** consumo de Kafka y procesamiento de los datos en microbatches.
- **Apache Parquet:** almacenamiento de los datos procesados.
- **Pandas:** cálculo de métricas y correlaciones.
- **PostgreSQL:** almacenamiento de los resultados numéricos.
- **Docker Compose:** ejecución de todos los servicios en contenedores.

La versión de Python y las bibliotecas utilizadas se encuentran en `Dockerfile` y `requirements.txt`.

## 4. Funcionamiento

El recorrido principal es el siguiente:

```text
Controller (Orquestación) -> Alpaca (Fuente) -> Memoria (Buffer) -> Kafka (Streaming) -> Streaming (Procesamiento de eventos) -> Spark (Procesamiento distribuido) -> Parquet (Persistencia de datos) -> Pandas (Análisis estadístico) -> PostgreSQL (Base de datos) -> Streamlit (Visualización)
```

El Controller administra cada ejecución, identificada por un `run_id`.

1. **Controller:** inicia la ejecución, coordina las etapas y registra su estado.
2. **Alpaca:** obtiene datos de una sesión bursátil anterior mediante ventanas temporales. En cada ventana consulta todos los grupos de símbolos, completa su paginación, combina y ordena los `TRADE` y `QUOTE` por sus timestamps originales y los agrupa por segundo bursátil histórico.
3. **Kafka:** recibe directamente un lote variable por cada segundo con eventos y publica sus registros individuales en un tópico. No se mezclan segundos distintos ni se introducen pausas: el segundo histórico no representa tiempo real de ejecución.
4. **Streaming:** permite que Spark consuma los eventos de Kafka de forma progresiva mediante Structured Streaming. No es un servicio adicional.
5. **Spark:** procesa los eventos en microbatches, distingue `TRADE` de `QUOTE` y escribe archivos Parquet. Se utiliza un Master y dos Workers.
6. **Parquet:** almacena los datos procesados que Pandas necesita para el análisis.
7. **Pandas:** lee los archivos Parquet y calcula las métricas y correlaciones.
8. **PostgreSQL:** guarda los resultados numéricos mediante inserciones o actualizaciones, evitando registros duplicados de un mismo RUN.
9. **Streamlit:** muestra las consolas, los gráficos y los resultados del análisis.

El Controller no transporta registros bursátiles: solo configura la conexión directa Alpaca-Kafka, coordina estados y recibe señales de primera publicación, error y finalización.

Si una etapa crítica falla, el Controller registra la causa y cancela coordinadamente el RUN completo mediante los eventos y callbacks existentes. Se detienen Alpaca, Kafka y Spark sin detener los servicios compartidos de Docker. El RUN queda en estado **Error**; el botón STOP mantiene su mecanismo independiente mediante SIGTERM y se muestra como detención manual.

Spark registra las operaciones de extracción, transformación, carga y análisis exploratorio (EXTRACT, TRANSFORM, LOAD y EDA). Durante una ejecución se generan fragmentos Parquet y se produce un archivo consolidado al finalizar. Si STOP reinicia Streamlit antes del cierre, la aplicación consolida los fragmentos pendientes al volver a iniciar.

### Recargas y detención global

F5 y las recargas automáticas normales de Streamlit no deben interrumpir un RUN activo. Los módulos de procesamiento activos se conservan durante las recargas y la interfaz recupera el estado desde los JSONL persistidos y los archivos TXT de cada etapa. La ausencia temporal de la pantalla no detiene por sí sola el procesamiento.

El botón **Detener streaming** es diferente: ejecuta `os.kill(os.getpid(), signal.SIGTERM)` y termina el proceso de Streamlit. Como el servicio `streamlit` tiene `restart: unless-stopped` en Docker Compose, Docker vuelve a iniciarlo automáticamente. Kafka Broker, Spark Master, Spark Workers y PostgreSQL no se reinician. La terminación elimina los threads Python de Alpaca, Controller y Kafka; al regresar, Streamlit recupera el RUN marcado como `manual_stop`, consolida el Parquet pendiente y recién después lo deja en estado finalizado. Si Spark demora en liberar recursos externos, la consolidación queda pendiente y se reintenta sin declarar éxito prematuramente.

## 5. Análisis realizado

La aplicación calcula, entre otros datos:

- Cantidad de operaciones por acción (`trade_count`).
- Volumen negociado (`total_volume`), obtenido de las operaciones `TRADE`.
- Cantidad de retornos disponibles (`return_count`).
- Volatilidad calculada a partir de los retornos (`volatility`).
- Cantidad de cotizaciones válidas (`spread_count`).
- Diferencia media entre `ask` y `bid` (`spread_mean`).

Para responder las dos preguntas utilizo la correlación de Pearson. La aplicación muestra las tablas con los valores por símbolo, las correlaciones y una interpretación descriptiva. También presenta un gráfico de las diez acciones con mayor volumen y otro de actividad durante la sesión.

No todos los símbolos tienen suficientes operaciones para calcular volatilidad. En esos casos puede aparecer `NULL`. Un valor igual a cero indica algo distinto: se obtuvo un resultado numérico de cero.

## 6. Organización del proyecto

- `main.py`: entrada de la aplicación Streamlit y orden de las secciones.
- `app/renders.py`: controles, consolas, información, gráficos, preguntas y componentes visuales compartidos.
- `app/functions.py`: funciones compartidas para rutas y logs.
- `app/database.py`: conexión y consultas PostgreSQL.
- `pipeline/`: los seis módulos de procesamiento, numerados del 01 al 06.
- `assets/`: estilos e imágenes.
- `compose.yaml` y `compose.override.yaml`: configuración de Docker.
- `Dockerfile` y `requirements.txt`: imagen y dependencias.
- `EJECUTAR en Windows.bat` y `EJECUTAR en Mac.command`: inicio de la aplicación.
- `AGENTS.md`: explicación técnica de la organización interna del proyecto.

La carpeta `data/` no se incluye en el repositorio. Es una carpeta de trabajo temporal que se crea cuando se inicia el proyecto. Allí se generan logs, metadatos, archivos Parquet y archivos de trabajo de los servicios. Puede eliminarse con los servicios detenidos para empezar una ejecución limpia. El volumen de PostgreSQL es independiente de esta carpeta.

## 7. Cómo ejecutarlo

### Windows

1. Descargar o descomprimir el proyecto.
2. Abrir Docker Desktop y comprobar que esté funcionando.
3. Ejecutar `EJECUTAR en Windows.bat`.
4. Esperar a que Docker construya las imágenes e inicie los servicios.
5. Abrir `http://localhost:8502`.
6. Elegir los símbolos e iniciar un RUN.

### macOS

Con Docker Desktop abierto, ejecutar `EJECUTAR en Mac.command` y acceder a la misma dirección.

No hace falta instalar Python, Java, Kafka, Spark, PostgreSQL ni crear un entorno virtual en la computadora. Las dependencias se instalan dentro de Docker. Para construir las imágenes y descargar los datos se necesita conexión a Internet.

PostgreSQL inicializa las tablas necesarias cuando corresponde. La primera ejecución no requiere archivos históricos ni una carpeta `data/` preparada de antemano.

## 8. Qué muestra la aplicación

La pantalla está organizada en configuración, dos bloques de consolas, información del RUN, gráficos y preguntas de investigación.

Las seis consolas corresponden al Controller, Alpaca, Kafka, Spark, Pandas y PostgreSQL. Muestran los últimos registros disponibles y permiten descargar el log completo de cada etapa.

La vista de cada consola muestra solamente las últimas 200 líneas. Esto no limita la descarga: cada botón lee el archivo TXT completo y descarga todas sus líneas, desde la primera hasta la última. Los seis archivos descargables son los logs de Controller, Alpaca, Kafka, Spark, Pandas y PostgreSQL.

Los resultados se identifican mediante el `run_id`. De esta forma se pueden consultar los datos de una ejecución sin mezclarlos con los de otra.

## 9. Limitaciones

- El feed IEX de Alpaca no representa todas las operaciones del mercado estadounidense.
- Los datos se descargan de una sesión histórica y se publican incrementalmente por ventanas, en lotes variables agrupados por el segundo bursátil original. Esto no es un feed en vivo ni una reproducción con velocidad controlada.
- Algunas acciones pueden tener pocas operaciones o cotizaciones y no permitir cálculos estadísticos completos.
- El rendimiento depende de los recursos asignados a Docker y de la disponibilidad de las APIs utilizadas.
- Una detención anticipada puede dejar fragmentos temporales mientras Spark libera recursos; Streamlit los consolida automáticamente al volver y reintenta si todavía no puede acceder a ellos.
- Las correlaciones describen asociaciones en los datos observados, no causalidad.

## 10. Conclusión

Apache Big Data Lab integra herramientas de adquisición, mensajería, procesamiento distribuido, análisis y almacenamiento en una misma aplicación. La interfaz facilita revisar el avance de cada etapa y consultar los resultados de las dos preguntas de investigación.

Las conclusiones estadísticas concretas dependen de los datos obtenidos en cada RUN y deben interpretarse teniendo en cuenta las limitaciones del feed y de la muestra.

## Fuentes

- Material teórico de las clases de Procesamiento de Datos.
- Ejemplos prácticos y código proporcionados por el profesor.
- Búsquedas y consultas en Internet.
- Videos de las clases y tutoriales en YouTube.
- Documentación oficial de las tecnologías utilizadas.
- ChatGPT como herramienta de apoyo para el desarrollo y la depuración del código.
