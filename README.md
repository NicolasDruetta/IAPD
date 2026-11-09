# Apache Big Data Lab

**Materia:** Procesamiento de Datos  
**Trabajo práctico:** TP2  
**Autor:** Nicolás Druetta  
**Año:** 2026

## 1. Descripción

Desarrollé esta aplicación para trabajar con datos bursátiles de Estados Unidos y practicar el procesamiento de datos con distintas herramientas. El programa descarga operaciones y cotizaciones históricas de Alpaca y las reproduce de forma progresiva para procesarlas con Kafka y Spark. Después calcula métricas con Pandas, las guarda en PostgreSQL y muestra los resultados en Streamlit.

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
Controller -> Alpaca -> Kafka -> Spark -> Parquet -> Pandas -> PostgreSQL -> Streamlit
```

El Controller administra cada ejecución, identificada por un `run_id`.

1. **Controller:** inicia la ejecución, coordina las etapas y registra su estado.
2. **Alpaca:** obtiene datos de una sesión bursátil anterior y los emite progresivamente.
3. **Kafka:** recibe y publica los eventos para que Spark pueda consumirlos.
4. **Spark:** procesa los eventos en lotes, distingue `TRADE` de `QUOTE` y escribe archivos Parquet. Se utiliza un Master y dos Workers.
5. **Pandas:** lee los resultados generados por Spark y calcula las métricas y correlaciones.
6. **PostgreSQL:** guarda los resultados mediante operaciones de inserción o actualización, evitando duplicar registros de un mismo RUN.

Spark registra las operaciones de extracción, transformación, carga y análisis exploratorio (EXTRACT, TRANSFORM, LOAD y EDA). Durante una ejecución se generan fragmentos Parquet y, cuando el cierre lo permite, se produce un archivo consolidado.

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

La carpeta `data/` no se incluye en la entrega. Es una carpeta de trabajo temporal que se crea cuando se inicia el proyecto. Allí se generan logs, metadatos, archivos Parquet y archivos de trabajo de los servicios. Puede eliminarse con los servicios detenidos para empezar una ejecución limpia. El volumen de PostgreSQL es independiente de esta carpeta.

## 7. Cómo ejecutarlo

### Windows

1. Descomprimir el RAR del proyecto.
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

Los resultados se identifican mediante el `run_id`. De esta forma se pueden consultar los datos de una ejecución sin mezclarlos con los de otra.

## 9. Limitaciones

- El feed IEX de Alpaca no representa todas las operaciones del mercado estadounidense.
- Los datos se descargan de una sesión histórica y después se reproducen como flujo.
- Algunas acciones pueden tener pocas operaciones o cotizaciones y no permitir cálculos estadísticos completos.
- El rendimiento depende de los recursos asignados a Docker y de la disponibilidad de las APIs utilizadas.
- Una detención anticipada puede dejar archivos parciales sin consolidar.
- Las correlaciones describen asociaciones en los datos observados, no causalidad.

## 10. Conclusión

El trabajo me permitió integrar herramientas de adquisición, mensajería, procesamiento distribuido, análisis y almacenamiento en una misma aplicación. La interfaz facilita revisar el avance de cada etapa y consultar los resultados de las dos preguntas de investigación.

Las conclusiones estadísticas concretas dependen de los datos obtenidos en cada RUN y deben interpretarse teniendo en cuenta las limitaciones del feed y de la muestra.

## Fuentes

- Material teórico de las clases de Procesamiento de Datos.
- Ejemplos prácticos y código proporcionados por el profesor.
- Búsquedas y consultas en Internet.
- Videos de las clases y tutoriales en YouTube.
- Documentación oficial de las tecnologías utilizadas.
- ChatGPT como herramienta de apoyo para el desarrollo y la depuración del código.
