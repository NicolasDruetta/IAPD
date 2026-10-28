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

Las constantes y variables definidas en el ámbito del módulo se ordenan
alfabéticamente dentro de sus respectivos bloques. Las variables locales se
ordenan según la secuencia lógica de la operación para conservar la claridad.

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

Cada archivo de `pages/` contiene exclusivamente funciones relacionadas con su
propia tecnología o funcionalidad. Las funciones comunes y reutilizables se
ubican en `main.py`.

Los módulos de `pages/` reutilizan las funciones comunes definidas en `main.py`.
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

`main.py` centraliza el punto de entrada, el Home, la navegación, la interfaz
de Streamlit, los controles, las tablas, los gráficos y las funciones
compartidas.

La carpeta `pages/` contiene el procesamiento específico de cada tecnología y
no construye interfaces de Streamlit independientes.

La carpeta `data/` contiene los datos de entrada y generados localmente,
`outputs/` contiene los resultados generados y `assets/` contiene los recursos
visuales.

Las funciones de nivel superior se ordenan alfabéticamente por nombre dentro
de cada módulo. Esta regla se aplica a `main.py` y a todos los módulos de
`pages/`, sin mezclar funciones entre archivos.

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

Los módulos de `pages/` utilizan una numeración progresiva que refleja el
orden real de implementación. Cada nueva tecnología recibe el siguiente
número disponible y no se crean archivos anticipadamente para etapas futuras.

## 12. Organización de archivos generados

Dentro de `/app/data/` se mantienen únicamente `historical/` y `streaming/`.
Cada RUN nuevo genera exactamente un JSONL en `historical/` y otro JSONL en
`streaming/`, con el mismo nombre base identificable por fecha, hora,
milisegundos y UUID. No se generan metadata, logs ni archivos auxiliares
separados dentro de `/app/data/`. Los archivos temporales de procesamiento se
ubican directamente en el almacenamiento temporal del contenedor y se limpian
por ejecución.

## 13. Logging operativo

Los logs operativos informan estados y contadores reales de las etapas de
procesamiento. El progreso se registra por grupos, ventanas o lotes, evitando
mensajes por registro individual. Los registros persistentes se agregan sin
truncar, sobrescribir ni descartar entradas silenciosamente. La consulta
visual utiliza lectura incremental, paginación o cursor, sin convertir ese
mecanismo en un límite de retención. Los heartbeats actualizan el estado de la
ejecución sin desplazar información de progreso útil.
