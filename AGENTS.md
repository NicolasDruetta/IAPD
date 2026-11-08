# Convenciones de programación — Apache Big Data Lab

## 1. Convenciones generales de Python

El código utiliza PEP 8 como estándar general, con las excepciones
particulares descritas en este documento. La indentación es de cuatro espacios
y la estructura debe mantenerse clara, modular y consistente.

Las anotaciones de tipos se utilizan cuando aportan claridad. La longitud de
línea preferente es de 79 caracteres.

## 2. Nomenclatura e identificadores

Los identificadores del proyecto se escriben exclusivamente en inglés.

- Las variables y funciones utilizan `snake_case`.
- Las constantes utilizan `UPPER_SNAKE_CASE`.
- Las clases utilizan `PascalCase`.
- Los módulos utilizan `snake_case`, respetando los nombres de archivo
  definidos.

Los nombres deben ser descriptivos y representar claramente la responsabilidad
de cada elemento.

Todas las listas se escriben inline, con todos sus elementos en una sola línea
por lista; no se divide un elemento por línea, salvo indicación explícita.

Las constantes y variables globales de todos los módulos Python se ordenan
dentro de sus respectivos bloques con esta regla:

- Primero se agrupan los identificadores que comienzan con `_`.
- Dentro de ese grupo se ordenan alfabéticamente ignorando el `_` inicial.
- Después se agrupan los identificadores sin `_` inicial.
- Dentro de ese grupo se ordenan alfabéticamente.

Las variables locales conservan el orden lógico de ejecución para mantener las
dependencias y la claridad de cada operación. Las variables globales que forman
parte de una secuencia ejecutable de inicialización conservan el orden necesario
para no alterar sus dependencias.

## 3. Organización de imports

La organización de imports utiliza el siguiente orden:

1. Todas las instrucciones `import ...`, juntas y ordenadas alfabéticamente.
2. Una línea vacía.
3. Todas las instrucciones `from ... import ...`, juntas y ordenadas
   alfabéticamente por módulo.
4. Una línea vacía antes de constantes, clases o funciones.

Las instrucciones `from` no aparecen antes de las instrucciones `import`. El
proyecto no utiliza `from __future__ import annotations`.

## 4. Formato y separación de líneas

El código admite como máximo una línea vacía consecutiva en cualquier archivo
Python. Esta convención se aplica entre imports, constantes, clases, funciones,
bloques de código y líneas internas de las funciones.

La separación entre funciones de nivel superior y funciones asíncronas sigue
el mismo criterio.

## 5. Funciones y decoradores

El proyecto no utiliza decoradores de Python, incluidos los decoradores de
Streamlit. Las funcionalidades equivalentes se expresan mediante funciones
convencionales y llamadas explícitas.

Cada archivo de `pipeline/` contiene exclusivamente funciones relacionadas con su
propia tecnología o funcionalidad. Las funciones comunes de logs y archivos se
ubican en `functions.py`, y las operaciones PostgreSQL en `database.py`.
La presentación de Streamlit se ubica en `main.py` y `renders.py`. El análisis
de Pandas y PostgreSQL se ubica en `pipeline/05_pandas_postgresql.py`, y las
consultas de datos para preguntas y gráficos se ubican en `renders.py`.

Los módulos de procesamiento `01` a `05` no contienen interfaz Streamlit.
El cargador del punto de entrada las proporciona al módulo especializado antes
de ejecutarlo, sin ejecutar una segunda instancia del HTML ni de los elementos
de Streamlit.

## 6. Comentarios y docstrings

Los comentarios y docstrings se redactan en español. Los identificadores
permanecen en inglés.

Los comentarios describen operaciones concretas y preferentemente utilizan la
primera persona, por ejemplo: «Obtengo», «Calculo», «Transformo» y «Muestro».
La documentación es breve, precisa y relacionada con la implementación.

## 7. Organización del código

`main.py` centraliza el punto de entrada y la interfaz Streamlit.
`pipeline/05_pandas_postgresql.py` implementa exclusivamente el análisis con
Pandas y la preparación de preguntas y gráficos. `database.py` implementa las
conexiones y operaciones PostgreSQL mediante instrucciones SQL; no calcula
datos. `renders.py` implementa la presentación, las consolas, sus logs y la
visualización de resultados analíticos.
`functions.py` centraliza la escritura y lectura compartida de logs.

Los módulos `01` a `05` procesan datos sin interfaz. `renders.py` consulta los
datos analíticos para construir las preguntas y los gráficos.

La carpeta `data/` contiene los datos de entrada y generados localmente,
`outputs/` contiene los resultados generados y `assets/` contiene los recursos
visuales.

Las funciones de nivel superior se ordenan alfabéticamente por nombre dentro
de cada módulo. Esta regla se aplica a `main.py` y a todos los módulos de
`pipeline/`, sin mezclar funciones entre archivos.

## 8. Buenas prácticas de desarrollo

El código mantiene una separación clara entre interfaz y procesamiento, un
manejo explícito de errores y un uso eficiente de la memoria.

Se evita la duplicación de código, las dependencias innecesarias y las
abstracciones que no aportan valor. Las soluciones priorizan la
reproducibilidad, la legibilidad, la mantenibilidad y la simplicidad técnica.

Los estilos visuales se definen exclusivamente en `assets/custom.css`, que se
carga desde `main.py`. No se utilizan atributos `style` ni bloques de estilos
inline en el código Python o en el HTML generado por la aplicación. Los
selectores CSS utilizan clases; no se utilizan atributos `data-testid` ni
otros atributos `data-*` como mecanismo de estilizado.

`assets/custom.css` conserva este orden de secciones: variables y temas,
controles del tema, etiquetas HTML, identificadores específicos y clases
personalizadas o de Streamlit. Las reglas nuevas se incorporan en la sección
que corresponde a su responsabilidad. Dentro de cada bloque de selectores,
las clases se ordenan alfabéticamente.

Los controles interactivos utilizan los estilos nativos de Streamlit por
defecto. No se crean clases ni reglas CSS específicas para botones, descargas
o alternadores de tema cuando el estilo nativo resuelve correctamente la
interfaz. Las personalizaciones se limitan a excepciones semánticas,
pequeñas y justificadas.

Por ejemplo, un botón común se implementa sin clases ni estilos adicionales:

```python
st.button("Iniciar streaming")
```

Un botón de descarga también conserva el estilo nativo y solo recibe una clave
cuando es necesario diferenciar varios widgets iguales:

```python
st.download_button(
    "↓",
    data=content,
    file_name=file_name,
    key=download_key,
)
```

Una acción destructiva puede utilizar una única clase específica cuando su
significado visual lo exige. El botón `danger` debe conservar fondo rojo,
borde rojo y texto e iconos blancos, tanto en tema claro como oscuro:

```python
st.button("■ Detener streaming", key="stop-button")
```

```css
.st-key-stop-button button {
    background: #dc3545 !important;
    border-color: #dc3545 !important;
    color: #ffffff !important;
}
```

No se agregan reglas equivalentes para cada botón, no se fuerzan colores
globales sobre todos los widgets y no se recrea mediante CSS un componente que
Streamlit ya ofrece de forma nativa.

## 9. Portabilidad y reproducibilidad

El proyecto debe poder ejecutarse en Windows y macOS a partir de las
dependencias declaradas en `requirements.txt`, sin depender de rutas
absolutas, configuraciones locales, certificados instalados manualmente ni
estados particulares de una computadora.

Las conexiones seguras utilizan almacenes de certificados proporcionados por
dependencias Python declaradas por el proyecto. La verificación SSL y la
validación del nombre de host permanecen activas.

## 10. Zonas horarias

Las conversiones de zona horaria destinadas a la presentación se centralizan
en una única función compartida ubicada en `main.py`. Esta función utiliza
`zoneinfo`, perteneciente a la biblioteca estándar de Python. Los módulos
individuales no duplican conversiones de zona horaria ni modifican los
timestamps originales durante la presentación.

## 11. Numeración de módulos

Los módulos de `pipeline/` utilizan una numeración progresiva que refleja el
orden real de implementación. Cada nueva tecnología recibe el siguiente
número disponible y no se crean archivos anticipadamente para etapas futuras.

## 12. Organización de archivos generados

Dentro de `/app/data/` cada RUN nuevo utiliza directamente el identificador
`YYYYMMDD_HHMMSS` y genera, cuando la etapa correspondiente se ejecuta,
`{run_id}_01_python_controller_info.jsonl`,
`{run_id}_01_python_controller_log.txt`,
`{run_id}_02_alpaca_stream_log.txt`,
`{run_id}_03_apache_kafka_log.txt`,
`{run_id}_04_apache_spark_data.parquet`,
`{run_id}_04_apache_spark_log.txt` y
`{run_id}_05_pandas_postgresql_log.txt`.
Los cinco logs canónicos son
`{run_id}_01_python_controller_log.txt`,
`{run_id}_02_alpaca_stream_log.txt`,
`{run_id}_03_apache_kafka_log.txt`,
`{run_id}_04_apache_spark_log.txt` y
`{run_id}_05_pandas_postgresql_log.txt`.
Structured Streaming conserva durante el RUN sus partes
Parquet y checkpoint temporales dentro de `spark/`, con el mismo identificador;
al finalizar
correctamente se consolida un único Parquet y se eliminan únicamente esos
temporales. Si el RUN falla, los temporales se conservan para recuperación y
diagnóstico.

Las rutas de archivos se construyen directamente con los nombres canónicos:
`01_python_controller_info`,
`01_python_controller_log`,
`02_alpaca_stream_log`,
`03_apache_kafka_log`,
`04_apache_spark_data`,
`04_apache_spark_log` y
`05_pandas_postgresql_log`.
Los archivos generados utilizan
`{run_id}_{numero}_{nombre_modulo}_{tipo}.{extension}`. El número y nombre
deben corresponder al módulo responsable dentro de `pipeline/`. No se utilizan
diccionarios de nombres, alias abreviados ni nombres de archivo alternativos.

Todas las URLs externas se declaran como constantes visibles al comienzo del
módulo que las utiliza. Las consultas no contienen URLs literales ocultas o
escritas directamente dentro de las funciones.

Los mapas de rutas utilizan nombres de archivo canónicos completos: cada
valor debe coincidir exactamente con el nombre generado después del
identificador del RUN. No se utilizan alias semánticos como nombres de
archivo; las referencias a datos, logs, información y Parquet deben resolver
directamente al nombre definido en esta sección.

## 13. Logging operativo

Los logs operativos informan estados y contadores reales de las etapas de
procesamiento. El progreso se registra por grupos, ventanas o lotes, evitando
mensajes por registro individual. Los registros persistentes se agregan sin
truncar, sobrescribir ni descartar entradas silenciosamente. La consulta
visual utiliza lectura incremental, paginación o cursor, sin convertir ese
mecanismo en un límite de retención. Los heartbeats actualizan el estado de la
ejecución sin desplazar información de progreso útil.

## 14. Textos visibles de la interfaz

Los textos visibles de la interfaz y de las consolas se escriben completos en
una sola línea de código. No se dividen cadenas literales entre varias líneas
ni se altera su contenido, incluidos espacios, tildes, puntuación y mayúsculas.

## 15. Servicios y contenedores

No se reinician, reconstruyen, recrean, detienen ni levantan servicios o
contenedores de Docker. Estas acciones requieren autorización expresa del
usuario en cada ocasión. Las verificaciones se realizan sin modificar el
estado operativo de los servicios.

## 16. Anchos fijos y orden de propiedades CSS

Los cinco botones de icono de la interfaz, el alternador de tema y los cinco
botones de descarga de logs, utilizan un ancho de 36 px y una altura de 40 px.
Para fijar el ancho de un control mediante CSS, utilizo únicamente la
propiedad `width` necesaria. No agrego declaraciones `min-width` ni
`max-width`; las condiciones `max-width` de las media queries se conservan
para mantener el comportamiento responsive.

Dentro de cada bloque CSS, las propiedades se ordenan por nombre: primero las
propiedades normales en orden alfabético, luego las propiedades con prefijo
simple como `-webkit-` y finalmente las variables CSS con prefijo `--`. Las
media queries y los selectores mantienen su organización por responsabilidad.
