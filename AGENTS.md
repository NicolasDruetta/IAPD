# Convenciones de desarrollo — Apache Big Data Lab

## 1. Convenciones generales de Python

El código Python utiliza PEP 8 como estándar general, con las excepciones particulares descritas en este documento. La indentación es de cuatro espacios y el código mantiene una estructura clara, modular y consistente.

Las anotaciones de tipos se utilizan cuando aportan claridad. La longitud de línea preferente es de 79 caracteres.

## 2. Nomenclatura e identificadores

Los identificadores del proyecto se escriben exclusivamente en inglés.

- Las variables y funciones utilizan `snake_case`.
- Las constantes utilizan `UPPER_SNAKE_CASE`.
- Las clases utilizan `PascalCase`.
- Los módulos utilizan `snake_case`, respetando los nombres de archivo definidos.

Los nombres son descriptivos y representan claramente la responsabilidad de cada elemento.

## 3. Organización de imports

La organización de imports utiliza el siguiente orden:

1. Todas las instrucciones `import ...`, juntas y ordenadas alfabéticamente.
2. Una línea vacía.
3. Todas las instrucciones `from ... import ...`, juntas y ordenadas alfabéticamente por módulo.
4. Una línea vacía antes de las constantes, clases o funciones.

Las instrucciones `from` no aparecen antes de las instrucciones `import`. Todas las instrucciones `from` forman un único bloque, sin líneas vacías entre módulos de la biblioteca estándar, bibliotecas externas o módulos locales. El proyecto no utiliza `from __future__ import annotations`.

## 4. Formato y separación de líneas

El código admite como máximo una línea vacía consecutiva en cualquier parte del archivo. Esta convención se aplica entre imports, constantes, clases, funciones, bloques de código y líneas internas de las funciones.

No se utilizan dos o más líneas vacías consecutivas. La separación entre funciones de nivel superior y funciones asincrónicas sigue el mismo criterio.

## 5. Funciones y decoradores

El proyecto prescinde de decoradores de Python (`@`), incluidos los decoradores de Streamlit. Las funcionalidades equivalentes se expresan mediante funciones convencionales y llamadas explícitas.

Cada archivo de `pages/` contiene exclusivamente funciones relacionadas con su propia tecnología o funcionalidad. Las funciones comunes y reutilizables se ubican en `main.py`.

## 6. Comentarios y docstrings

Los comentarios y docstrings se redactan en español. Los identificadores permanecen en inglés.

Los comentarios describen operaciones concretas y preferentemente utilizan la primera persona, por ejemplo: «Obtengo», «Calculo», «Transformo» y «Muestro». La documentación es breve, precisa y está relacionada con la implementación.

## 7. Organización del código

`main.py` centraliza el punto de entrada, el Home, la navegación, la interfaz de Streamlit, los controles, las tablas, los gráficos y las funciones compartidas.

La carpeta `pages/` contiene el procesamiento específico de cada tecnología y no construye interfaces de Streamlit independientes.

La carpeta `data/` contiene los datos de entrada, `outputs/` contiene los resultados generados y `assets/` contiene los recursos visuales.

## 8. Buenas prácticas de desarrollo

El código mantiene una separación clara entre interfaz y procesamiento, un manejo explícito de errores y un uso eficiente de la memoria.

Se evita la duplicación de código, las dependencias innecesarias y las abstracciones que no aportan valor. Las soluciones priorizan la reproducibilidad, la legibilidad, la mantenibilidad y la simplicidad técnica.

Los estilos visuales se definen exclusivamente en `assets/custom.css`, que se
carga desde `main.py`. No se utilizan atributos `style` ni bloques de estilos
inline en el código Python o en el HTML generado por la aplicación.

## 9. Portabilidad y reproducibilidad

El proyecto debe poder ejecutarse en Windows y macOS a partir de las dependencias declaradas en `requirements.txt`, sin depender de rutas absolutas, configuraciones locales, certificados instalados manualmente ni estados particulares de una computadora.

Las conexiones seguras utilizan almacenes de certificados proporcionados por dependencias Python declaradas por el proyecto. La verificación SSL y la validación del nombre de host permanecen activas.
