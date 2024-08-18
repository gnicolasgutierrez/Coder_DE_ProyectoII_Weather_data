# *Descripción del Proyecto:

Este proyecto se enfoca en la ingeniería de datos, específicamente en la integración y análisis de datos meteorológicos. Utilizando la API de OpenWeatherMap, el proyecto extrae datos del clima y los almacena en una base de datos Amazon Redshift. La solución incluye un proceso ETL (Extracción, Transformación y Carga) implementado en Python, que utiliza bibliotecas como requests, pandas, sqlalchemy, y dotenv.

El flujo de trabajo del proyecto incluye:

Conexión a la API: Obtención de datos meteorológicos en tiempo real desde OpenWeatherMap.
Transformación de Datos: Limpieza y preparación de los datos utilizando pandas, manejando valores nulos, duplicados y atípicos.
Carga de Datos: Inserción de los datos procesados en una base de datos Redshift utilizando sqlalchemy.
Configuración de Base de Datos: Creación de tablas en Redshift con una clave primaria compuesta por las columnas 'city' y 'timestamp'.
Manejo de Errores y Logging: Implementación de un sistema de logging para seguimiento y manejo de excepciones.
El proyecto está diseñado para proporcionar una infraestructura robusta para el almacenamiento y análisis de datos climáticos, facilitando la toma de decisiones basada en información meteorológica precisa y actualizada.
