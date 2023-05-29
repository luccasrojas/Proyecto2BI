# Proyecto 2 - Inteligencia de Negocios

## Integrantes

- Tony Montes 
- Luccas Rojas
- Brian Rivera

## Descripción

El proyecto consiste en la implementación de un proceso ETL y una arquitectura de solución para la respuesta de ciertos análisis sobre los factores que más influyen en la presencia de Asma en la población colombiana.

Los datos utilizados fueron los de la encuesta multipropósito realizada por el DANE en el año 2017 en Colombia. La información de estos datos está disponible [aquí](https://microdatos.dane.gov.co/index.php/catalog/565/data-dictionary).

## Ejecución

Para ejecutar el proceso ETL, primero se deben incluir los datos con Asma y los datos con Asma (CSV) en la carpeta [`data`](data/), ya que estos archivos no se encuentran en el repositorio por su tamaño.

Luego, se deben configurar las variables de usuario, contraseña y demás en el archivo [`load.py`](code/load.py), para permitir la conexión con la base de datos de PostgreSQL.

Finalmente, se debe ejecutar el archivo `job.py` en [esta ubicación](code/job.py).
