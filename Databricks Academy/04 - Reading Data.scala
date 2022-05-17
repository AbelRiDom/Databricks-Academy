/*Leer y escribir datos y tablas
Objetivos de aprendizaje
Al final de estas lecciones, usted debería ser capaz de:

Utilice Dataframes para:
Leer datos en formato CSV
Defina y use esquemas mientras carga datos
Escribir datos en Parquet
Guardar datos en tablas
Registrar vistas
Leer datos de tablas y vistas de nuevo a DataFrames
Describir similitudes y diferencias entre DataFrames, vistas y tablas con respecto a la persistencia y el alcance.
*/

/*Use %fs head ... para ver las primeras líneas del archivo.*/
%fs head /mnt/training/iris/iris.csv
/*"","Sepal.Length","Sepal.Width","Petal.Length","Petal.Width","Species"
"1",5.1,3.5,1.4,0.2,"setosa"
"2",4.9,3,1.4,0.2,"setosa"
"3",4.7,3.2,1.3,0.2,"setosa"
"4",4.6,3.1,1.5,0.2,"setosa"
"5",5,3.6,1.4,0.2,"setosa"
"6",5.4,3.9,1.7,0.4,"setosa"
"7",4.6,3.4,1.4,0.3,"setosa"
"8",5,3.4,1.5,0.2,"setosa"
"9",4.4,2.9,1.4,0.2,"setosa"
"10",4.9,3.1,1.5,0.1,"setosa"
"11",5.4,3.7,1.5,0.2,"setosa"
"12",4.8,3.4,1.6,0.2,"setosa"
"13",4.8,3,1.4,0.1,"setosa"
"14",4.3,3,1.1,0.1,"setosa"
"15",5.8,4,1.2,0.2,"setosa"
"16",5.7,4.4,1.5,0.4,"setosa"
"17",5.4,3.9,1.3,0.4,"setosa"
"18",5.1,3.5,1.4,0.3,"setosa"
"19",5.7,3.8,1.7,0.3,"setosa"
"20",5.1,3.8,1.5,0.3,"setosa"*/

/*La siguiente es la sintaxis estándar para cargar datos en DataFrames, aquí con algunas opciones configuradas específicamente para archivos CSV.*/
/*El delimitador predeterminado para leer CSV es una coma (,), pero es posible cambiarlo con la opción "delimitador" para extender este método para cubrir archivos usando barras verticales, punto y coma, tabuladores u otros separadores personalizados.*/
val csvFilePath = "/mnt/training/iris/iris.csv"

val tempDF = spark.read
  .format("csv")
  .option("header", true)
  .option("inferSchema", true)
  .load(csvFilePath)

 

  /*Tenga en cuenta las opciones que se utilizan
Format
El método de formato especifica qué tipo de datos se están cargando. El formato predeterminado en Spark es Parquet. Este método brinda acceso a docenas de formatos de archivo, así como conexiones a una multitud de servicios conectados; una lista bastante completa está disponible aquí. Nota al margen Algunos formatos tienen métodos con nombre (json, csv, parquet) que se pueden usar en lugar de load(filePath), por ejemplo, spark.read.csv(filePath). Estos métodos son análogos a especificar el formato usando el método de formato, pero menos extensibles.

"header"
La opción "header" le dice al DataFrame que use la primera fila para los nombres de las columnas. Sin esta opción, a las columnas se les asignarán nombres anónimos _c0, _c1, ... _cN. El DataFrameReader mira la primera línea del archivo para capturar esta información, activando un job. "inferSchema" La opción "inferSchema" escaneará el archivo para asignar tipos a cada columna. Sin esta opción, a todas las columnas se les asignará el StringType. El DataFrameReader escaneará todo el contenido del archivo para determinar qué tipo inferir, activando un job.*/

/*Imprimir el esquema
Ejecute el comando printSchema() para ver la estructura del DataFrame.

El nombre de cada columna y su tipo se imprimen en el cuaderno.

Nota al margen Otras funciones de DataFrame se cubren en las siguientes lecciones.*/

tempDF.printSchema()
/*root
 |-- _c0: integer (nullable = true)
 |-- Sepal.Length: double (nullable = true)
 |-- Sepal.Width: double (nullable = true)
 |-- Petal.Length: double (nullable = true)
 |-- Petal.Width: double (nullable = true)
 |-- Species: string (nullable = true)*/

 /*
 Del aviso de esquema:

Hay 6 columnas en el DataFrame.
Se dedujeron los tipos correctos para cada columna.
A la primera columna se le asignó el nombre anónimo _c0 porque no había un nombre de campo válido en el CSV.
Nuestros otros nombres de columna contienen letras mayúsculas y puntos.*/

/*
Proporcionar un esquema al leer de un CSV
Un Job se activa cada vez que se requiere "físicamente" que toquemos los datos. Nuestra lectura anterior activó 2 trabajos, uno para mirar el encabezado y el segundo para inferir los tipos a través de un análisis completo del archivo.

Definir y especificar un esquema evitará que se activen trabajos al definir una operación de lectura.

Declarar el esquema
Un esquema es una lista de nombres de campos y tipos de datos. Tenga en cuenta que este ejemplo anula los nombres proporcionados por el archivo; cuando se trabaja con CSV, el esquema se aplica a las columnas en el orden en que aparecen.

Precaución Muchos tipos de archivos compararán los nombres y tipos de campos definidos por el usuario con los codificados en el archivo y fallarán si se pasa un esquema incorrecto. */

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

lazy val csvSchema = StructType(
  StructField("index", IntegerType) ::
  StructField("sepal_length", DoubleType) ::
  StructField("sepal_width", DoubleType) ::
  StructField("petal_length", DoubleType) ::
  StructField("petal_width", DoubleType) ::
  StructField("species", StringType) :: Nil)

  /*
  Leer los datos
Pase el objeto de esquema definido anteriormente utilizando el método schema().

Nota al margen Todavía tenemos que especificar la opción "encabezado" como verdadera. Ya hemos proporcionado los nombres de columna correctos en nuestra definición de esquema, 
pero debemos informarle a Spark que la primera fila del archivo no debe leerse como datos reales.*/

val irisDF = spark.read
 .option("header", "true")
 .schema(csvSchema)          // Use the specified schema
 .csv(csvFilePath)
 

 /*
 Registrar una vista temporal
Las vistas temporales en Spark son esencialmente el equivalente a DataFrames para SQL. Ni las vistas temporales ni los DataFrames persistirán entre cuadernos/ejecuciones de trabajos. 
Las vistas temporales solo se agregan al catálogo de Spark en lugar de al metastore.

Es fácil crear una vista temporal desde un DataFrame. Esto brinda la capacidad de cambiar fácilmente entre la API de DataFrames y SQL.
*/

irisDF.createOrReplaceTempView("iris_temp")

/*
A continuación, eche un vistazo a los datos con una simple instrucción SQL SELECT:
*/

%sql

SELECT * FROM iris_temp

/*
Escribir en archivos
En muchos casos, los cambios aplicados a través de DataFrame o acciones de SQL deberán persistir. Al escribir archivos en el disco,
estos datos pueden transferirse fácilmente entre sesiones y compartirse con otros usuarios.

El formato de archivo de parquet
Parquet es el formato de archivo predeterminado cuando se trabaja con Spark. Parquet es un formato de columnas que es compatible con muchos sistemas de procesamiento de datos. 
Spark está optimizado para realizar operaciones en archivos de parquet (tenga en cuenta que el formato Delta Lake está construido sobre el parquet). 
Spark SQL brinda soporte para leer y escribir archivos de Parquet que conservan automáticamente el esquema de los datos originales. 
Al escribir archivos de Parquet, todas las columnas se convierten automáticamente para que admitan valores NULL por motivos de compatibilidad.



Opciones de escritura
Hay muchas opciones de escritura, y escribir en algunos servicios integrados requerirá que se aprueben opciones esotéricas específicas. 
La sintaxis en la celda a continuación es el ejemplo más mínimo pero explícito del uso de DataFrames para guardar datos. 
Aquí, los datos y el esquema actualmente asociados con irisDF se conservarán en el directorio especificado por outputFilePath.

format
Al igual que DataFrameReader, DataFrameWriter acepta una amplia gama de formatos. También admite el uso de algunos métodos específicos de archivo (json, parquet, csv) en lugar de la sintaxis .save.

mode
Spark tiene varios modos de guardado.

mode	    details
"error"	    DEFAULT; generará un mensaje de error si ya existen datos en la ruta especificada.
"overwrite"	Si existen datos en la ruta de destino, se eliminarán antes de que se guarden los nuevos datos. 
            (Esto se usa mucho a lo largo del curso para que las lecciones o las celdas individuales se puedan volver a ejecutar sin conflicto).
"append"	Si existen datos en la ruta de destino, los nuevos datos que se guardarán se agregarán a los datos existentes.
"ignore"	Si existen datos en la ruta de destino, NO se guardarán datos nuevos.
*/
val outputFilePath = s"$userhome/iris"

irisDF
  .write
  .format("parquet")
  .mode("overwrite")
  .save(outputFilePath)

/*
Al escribir con Spark, la ruta de escritura de destino siempre será un directorio de archivos. 
Los archivos que contienen datos específicamente tendrán la extensión que coincida con el formato especificado, 
mientras que otros archivos en este directorio son necesarios para la concurrencia y la tolerancia a fallas.
*/

display(dbutils.fs.ls(outputFilePath))
/*
path	                                                                                                            |name	                                                                                                |size
dbfs:/user/abel.rivera@teia.mx/iris/_SUCCESS	                                                                    |_SUCCESS	                                                                                            |0
dbfs:/user/abel.rivera@teia.mx/iris/_committed_2349057410651505842	                                                |_committed_2349057410651505842	                                                                        |238
dbfs:/user/abel.rivera@teia.mx/iris/_committed_4748615211757343386	                                                |_committed_4748615211757343386	                                                                        |126
dbfs:/user/abel.rivera@teia.mx/iris/_started_2349057410651505842	                                                |_started_2349057410651505842	                                                                        |0
dbfs:/user/abel.rivera@teia.mx/iris/_started_4748615211757343386	                                                |_started_4748615211757343386	                                                                        |0
dbfs:/user/abel.rivera@teia.mx/iris/part-00000-tid-2349057410651505842-4e45aa65-1510-4b14-84f4-ea4188259a53-17557-1 |part-00000-tid-2349057410651505842-4e45aa65-1510-4b14-84f4-ea4188259a53-17557-1-c000.snappy.parquet	|3529
-c000.snappy.parquet	
*/

/*
Registro de tablas en databricks
Databricks nos permite "registrar" el equivalente de "tablas" para que todos los usuarios puedan acceder fácilmente a ellas.

Los beneficios de registrar una tabla en un espacio de trabajo incluyen:

Las tablas persisten entre cuadernos y sesiones
Está disponible para cualquier usuario en la plataforma (si los permisos lo permiten)
Minimiza la exposición de las credenciales
Más fácil de anunciar conjuntos de datos disponibles a otros usuarios
Las ACL se pueden configurar en tablas para controlar el acceso de los usuarios en un espacio de trabajo
Registrar una tabla con la interfaz de usuario de Databricks
La interfaz de usuario de Databrick también tiene compatibilidad integrada para trabajar con varias fuentes de datos y tablas de registro diferentes.

Podemos cargar el archivo CSV aquí y registrarlo como una tabla usando la interfaz de usuario.

Registrar una tabla mediante programación
La siguiente celda registra una tabla utilizando los datos de Parquet escritos en el paso anterior. No se copian datos durante este proceso.

Los documentos de Databricks sobre la creación de tablas mediante programación detallan este método, así como el método dataFrame.write.saveAsTable("table-name"). 
Tenga en cuenta que el último método siempre escribirá datos en el directorio de destino (independientemente de si la creación de la tabla es administrada o no). 
Estos conceptos se explorarán en el siguiente laboratorio.

Tanto scala como python tienen un método spark.sql() que permite ejecutar comandos SQL arbitrarios en sus respectivos núcleos. 
Esto es especialmente útil porque permite que las cadenas almacenadas como variables se pasen mediante programación a los comandos SQL.
*/

val tableName = "iris"

spark.sql(s"DROP TABLE IF EXISTS $tableName")

spark.sql(s"""
  CREATE TABLE $tableName
  USING PARQUET
  LOCATION "$outputFilePath"
""")

/*
Tabla de consultas
Dentro del NOTEBOOK en el que están definidos, la interacción con las tablas y las vistas es idéntica: solo las consultamos con SQL. 
La diferencia para recordar es que las tablas persisten entre sesiones y (de forma predeterminada) están disponibles para todos los usuarios en el espacio de trabajo.

Haga clic en el ícono de Datos en la barra lateral izquierda para ver todas las tablas en su base de datos.

Veamos una vista previa de nuestros datos. 
*/

display(spark.sql(s"SELECT * FROM $tableName"))

/*
Leer desde una tabla/vista
Volver a transformar una tabla o vista en un DataFrame es simple y no desencadenará un trabajo. 
Los metadatos asociados con la tabla simplemente se reasignan al marco de datos, por lo que ahora se puede acceder a los mismos permisos de acceso y esquema a través de los marcos de datos y las API de SQL.
*/

lazy val newIrisDF = spark.table("iris")
newIrisDF.printSchema()

/*
root
 |-- index: integer (nullable = true)
 |-- sepal_length: double (nullable = true)
 |-- sepal_width: double (nullable = true)
 |-- petal_length: double (nullable = true)
 |-- petal_width: double (nullable = true)
 |-- species: string (nullable = true)
 */

 /*
 Crear una vista temporal con SQL
Anteriormente, creamos un registro de una vista temporal usando la API de DataFrame y es igual de fácil registrar vistas temporales usando consultas SQL. 
Tenga en cuenta que no se activa ningún trabajo en la definición de vista.
*/
%sql

CREATE OR REPLACE TEMP VIEW setosas
AS (SELECT *
  FROM iris
  WHERE species = "setosa")

/*
Tenga en cuenta que la tabla SELECT * FROM produce el mismo resultado que display(df) si trabaja con un DataFrame. 
La próxima lección discutirá por qué mostrar el contenido de esta vista toma más tiempo que crearlo.
*/

%sql

SELECT * FROM setosas

/*
Revisar el catálogo de Spark
La lista de tablas dentro del catálogo de Spark demuestra que las vistas temporales se tratan de la misma manera que las tablas, pero no están asociadas con una base de datos. 
Las vistas temporales no persistirán entre sesiones de Spark (por ejemplo, cuadernos), mientras que la tabla persistirá como parte de la base de datos en la que se registró.
*/

display(spark.catalog.listTables())

/*
Conclusiones clave
Los DATAFRAME, las tablas y las vistas brindan un acceso sólido a los cálculos de Spark optimizados.
Los DATAFRAME y las vistas proporcionan un acceso temporal similar a los datos; las tablas conservan estos patrones de acceso al espacio de trabajo.
No se ejecuta ningún trabajo al crear un DataFrame a partir de una tabla: el esquema se almacena en la definición de la tabla en Databricks.
Los tipos de datos inferidos o establecidos en el esquema se conservan cuando se escriben en archivos o se guardan en tablas.
Las ACL se pueden usar para controlar el acceso a las tablas dentro del espacio de trabajo.
*/