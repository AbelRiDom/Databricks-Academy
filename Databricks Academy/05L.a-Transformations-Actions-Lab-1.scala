/*
Laboratorio de Transformaciones y Acciones
Objetivos de aprendizaje
En este laboratorio, usted:

Explore los datos para comprender los contenidos
Validar la consistencia de los datos
Derivar nuevos campos del formato de datos actual
Convertir campos donde sea necesario
Calcular agregados
Guardar datos agregados
Habilidades exploradas
Explore y use DataFrames y transformaciones SQL
Familiarícese con varios métodos en spark.sql.functions leyendo los documentos de la API
Revise cómo se activan los trabajos en asociación con las acciones
Reforzar los conceptos de cómo Spark ejecuta la lógica en una fuente de datos
*/
/*
Empezando 
Ejecute la siguiente celda para configurar nuestro "classroom".
*/

%run ./Includes/Classroom-Setup

/*
Resumen de los datos
Esta práctica de laboratorio reutiliza los datos meteorológicos de la práctica de laboratorio anterior.

Los datos incluyen múltiples entradas de una selección de estaciones meteorológicas, incluidas las temperaturas promedio registradas en grados Fahrenheit o Celcius. 
El esquema de la tabla:

ColumnName	DataType	Description
NAME	    string	    Station name
STATION	    string	    Unique ID
LATITUDE	float	    Latitude
LONGITUDE	float	    Longitude
ELEVATION	float	    Elevation
DATE	    date	    YYYY-MM-DD
UNIT	    string	    Temperature units
TAVG	    float	    Average temperature

Si bien el número total de filas en este conjunto de datos haría que la exploración manual fuera extremadamente ineficiente, 
muchas agregaciones de estos datos producen una salida lo suficientemente pequeña para la revisión manual.
*/

/*
Registrar tabla y cargar datos en DataFrame
La siguiente celda vuelve a ejecutar la lógica del último laboratorio y garantiza que todos los estudiantes tendrán el mismo entorno.

Un desglose de las operaciones
1-La primera borra la tabla weather si existe. Esto garantiza que no se produzcan conflictos en el metastore.

2-El esquema se define en SQL DDL. Tenga en cuenta que los nombres de las columnas se proporcionan en mayúsculas para que coincidan con el formato de los archivos de origen.

3-sourcePath especifica los datos de parquet que se leerán. El directorio de origen es de solo lectura y se montó anteriormente cuando se ejecutaba el programa 
"Includes/Classroom-Setup".

4-La variable tablePath especifica dónde se almacenarán los archivos asociados con la tabla no administrada. La parte de inicio de usuario apunta a un directorio creado 
con el nombre de usuario de cada estudiante en el almacén de objetos predeterminado (DBFS raíz) asociado con el área de trabajo de Databricks.

5-El bloque de código de varias líneas que incluye spark.read...write...saveAsTable especifica un formato de origen, un esquema y una ruta de lectura. Los datos del origen se 
copian en el tablePath de destino, sobrescribiendo cualquier dato que ya exista en ese directorio.La tabla weather es registrada en los archivos especificados en la ruta de 
destino mediante el esquema proporcionado. Tenga en cuenta que esta lógica aprovecha que parquet es el formato predeterminado al escribir con Spark (los datos escritos en 
tablePath serán archivos de parquet).

6-La última línea crea un DataFrame a partir de la tabla meteorológica.
Table weather y el DataFrame weatherDF comparten las mismas definiciones de metadatos, 
lo que significa que tanto el esquema como los archivos a los que se hace referencia son idénticos.
Esto proporciona un acceso análogo a los datos a través de la API de DataFrames o Spark SQL. 
Los cambios en los datos guardados en tablePath se reflejarán inmediatamente en consultas posteriores a través de cualquiera de estas API.
*/
spark.sql("DROP TABLE IF EXISTS weather")

val schemaDDL = "NAME STRING, STATION STRING, LATITUDE FLOAT, LONGITUDE FLOAT, ELEVATION FLOAT, DATE DATE, UNIT STRING, TAVG FLOAT"

val sourcePath = "/mnt/training/weather/StationData/stationData.parquet/"

val tablePath = s"$userhome/weather"

spark.read
  .format("parquet")
  .schema(schemaDDL)
  .load(sourcePath)
  .write
  .option("path", tablePath)
  .mode("overwrite")
  .saveAsTable("weather")

val weatherDF = spark.table("weather")

/*
Importar funciones necesarias
Según las preferencias del usuario, este laboratorio se puede completar con SQL, Python o Scala. Recuerde que las consultas SQL y la API de DataFrames se pueden unir 
mediante el uso de spark.sql, que devolverá un objeto DataFrame.

Muchos de los métodos utilizados para las transformaciones de DataFrame se encuentran dentro del módulo sql.functions. Los enlaces a los documentos de la API de Scala y 
Python se proporcionan aquí:

Documentos de la API de pyspark.sql.functions
https://spark.apache.org/docs/latest/api/python/index.html
Documentos de API de Scala spark.sql.functions
https://spark.apache.org/docs/2.2.1/api/java/org/apache/spark/sql/functions.html
Tenga en cuenta que los métodos en cada uno se compilarán en el mismo plan en ejecución. Algunas personas informan que la navegación de los documentos de Scala es más fácil,
incluso cuando se codifica en Python.

Si codifica en SQL, las funciones integradas se pueden encontrar aquí.
https://spark.apache.org/docs/2.3.1/api/sql/index.html

Extendiéndose en SQL, la mayoría de las operaciones de Hive DML también son compatibles; documentos completos aquí.
https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML

La celda de abajo actualmente solo importa la función col. Siéntase libre de agregar a la lista de importación y volver a ejecutar esta celda, o importar funciones según sea 
necesario más adelante en el laboratorio.
*/

import org.apache.spark.sql.functions.{col} // agregue métodos adicionales como una lista separada por comas dentro de {}

// Alternativamente, puede importar todas las funciones comentando y ejecutando la siguiente línea:

// importar org.apache.spark.sql.functions._

/*
Vista previa de 20 líneas de datos
Comience mostrando 20 líneas de datos para tener una idea de cómo están formateados los datos.

limit, show, head y take lograrán esto, pero desencadenarán diferentes números de jobs.
*/

display(weatherDF.limit(20))

/*
Limitar la vista de los datos a 20 líneas es una transformación, pero cada vez que se devuelven los datos a la pantalla, se activará una acción (y al menos 1 trabajo).
*/

/*
Definir un nuevo DataFrame o vista que contenga todos los nombres distintos
Debido a la evaluación diferida, los DataFrames y las vistas no se ejecutan hasta que se ejecuta una acción contra ellos.

El registro de vistas temporales intermedias o DataFrames esencialmente permite dar un nombre a un conjunto de transformaciones contra un conjunto de datos.
Esto puede ser útil cuando se crea una lógica compleja, ya que no se replicarán datos cuando se utilice un estado intermedio varias veces en consultas posteriores.

Use el comando distinto en la columna NOMBRE y guarde el resultado en un nuevo DataFame llamado uniqueNamesDF. Tenga en cuenta que su definición no debe desencadenar un trabajo.
*/

val uniqueNamesDF = weatherDF.select("name").distinct()


/*
Ahora devuelva el conteo y muestre los nombres únicos. Cada uno de estos es una acción separada.

display() suprimirá cualquier salida de la consola, así que haz esto en 2 celdas.
*/

display(weatherDF.groupBy("name").count())

/*
Nuevamente, la evaluación lazy en Spark espera hasta que estos resultados deban devolverse para activar un trabajo.
*/

/*
Confirmar la consistencia de la información de la estación
Los campos NOMBRE, ESTACIÓN, LATITUD, LONGITUD y ELEVACIÓN deben permanecer consistentes para cada estación única a lo largo de los datos.
Si esto es cierto, el recuento de nombres distintos debería ser equivalente al recuento de las combinaciones distintas de estas cinco columnas en los datos actuales.
Escriba una consulta que confirme esto utilizando el DataFrame o la vista definida en el paso anterior como punto de referencia.
*/