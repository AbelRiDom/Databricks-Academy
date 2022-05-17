/*
Laboratorio de lectura y escritura de datos y tablas
Objetivos de aprendizaje
En este laboratorio, usted:

Utilice Dataframes para:
Leer datos de formato parquet
Defina y use esquemas mientras carga datos
Escribir datos en Parquet
Guardar datos en tablas
Registrar vistas
Leer datos de tablas y vistas de nuevo a DataFrames
(OPCIONAL) Explore las diferencias entre tablas administradas y no administradas
Recursos

Guía de Spark SQL, Dataframes y conjuntos de datos
https://spark.apache.org/docs/2.2.0/sql-programming-guide.html
Funciones de carga y guardado de Spark
https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html
Bases de datos y tablas - Databricks Docs
https://docs.databricks.com/user-guide/tables.html
Tablas administradas y no administradas
https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables
Creación de una tabla con la interfaz de usuario
https://docs.databricks.com/user-guide/tables.html#create-a-table-using-the-ui
Crear una tabla local
https://docs.databricks.com/user-guide/tables.html#create-a-local-table
Guardar en tablas persistentes
https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#saving-to-persistent-tables
Documentos de DataFrame Reader
https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameReader
Documentos de DataFrame Writer
https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrameWriter
*/

/*
Getting Started
Run the following cell to configure our "classroom."
*/

%run ./Includes/Classroom-Setup

/*
Leer Parquet en un DataFrame
Cargue los datos en la variable sourcePath en un DataFrame llamado tempDF. No es necesario establecer opciones más allá de especificar el formato.
*/

val sourcePath = "/mnt/training/weather/StationData/stationData.parquet/"

val tempDF = spark.read
  .format("parquet")
  .option("header", true)
  .option("inferSchema", true)
  .load(sourcePath)
/*
tempDF:org.apache.spark.sql.DataFrame
NAME:string
STATION:string
LATITUDE:float
LONGITUDE:float
ELEVATION:float
DATE:date
UNIT:string
TAVG:float
*/

/*
Revisar y definir el esquema
Tenga en cuenta que se activó un solo job. Con los archivos de parquet, se registra el esquema de cada columna, pero Spark aún debe mirar el archivo para leer esta información 
(de ahí el job).

Para evitar desencadenar este job, se puede pasar un esquema como argumento. Defina el esquema aquí usando tipos y campos SQL DDL o Spark (como se demostró en la lección anterior).
*/
import org.apache.spark.sql.types.{StructType,StructField,StringType,FloatType,DateType}

lazy val parquetSchema = StructType(
StructField("NAME",StringType) ::
StructField("STATION",StringType) ::
StructField("LATITUDE",FloatType) ::
StructField("LONGITUDE",FloatType) ::
StructField("ELEVATION",FloatType) ::
StructField("DATE",DateType) ::
StructField("UNIT",StringType) ::
StructField("TAVG",FloatType) :: Nil)

/*
Cargar datos con esquema definido
No se activará ningún job cuando se proporcione una definición de esquema. Cargue los datos de sourcePath en un DataFrame llamado weatherDF.
*/

val weatherDF = spark.read
 .option("header", "true")
 .schema(parquetSchema)       
 .parquet(sourcePath)

/*
Guardar una tabla no administrada
El método de DataFrame saveAsTable registra los datos a los que se hace referencia actualmente en DataFrame en el metaalmacén y guarda una copia de los datos.

Si no se proporciona una "ruta", Spark creará una tabla administrada, lo que significa que tanto los metadatos como los datos se copian y almacenan en el DBFS de almacenamiento raíz asociado
 con el espacio de job. Esto significa que eliminar o modificar la tabla modificará los datos en el DBFS. Una tabla no administrada permite desacoplar los datos y los metadatos, 
 por lo que se puede cambiar el nombre de una tabla o eliminarla fácilmente del espacio de job sin eliminar o migrar los datos subyacentes.

Guarde weatherDF como una tabla no administrada. Utilice el tablePath proporcionado con la opción "ruta". Establezca el modo en "sobrescribir" (lo que eliminará la tabla si existe actualmente).
Pase el nombre de la tabla "weather" al método saveAsTable.
*/
val tablePath = s"$userhome/weather"

weatherDF
.write
.mode("overwrite")
.option("path",tablePath)
.saveAsTable("weather")

  /*
  Tabla de consultas
Esta tabla contiene los mismos datos que el weatherDF. Recuerde que las tablas persisten entre sesiones y (de manera predeterminada) están disponibles para todos los usuarios en el espacio de job.

Veamos una vista previa de nuestros datos.
*/

%sql

select * from weather

/*
Crear una vista temporal con SQL
Es fácil registrar vistas temporales mediante consultas SQL. Las vistas temporales esencialmente permiten dar un nombre a un conjunto de transformaciones SQL contra un conjunto de datos.
Esto puede ser útil cuando se crea una lógica compleja o cuando se utilizará un estado intermedio varias veces en consultas posteriores. Tenga en cuenta que no se activa ningún job en la definición de vista.

Utilice SQL para crear una vista temporal denominada station_counts que devuelva el recuento de las temperaturas promedio registradas tanto en F como en C para cada estación, ordenadas por nombre de estación.
-AGRUPACION POR ESTACION Y POR UNIDAD
-LOS CPUT VAN DE LA MANO CON GROUP BY

Precaución COUNT creará el nombre de columna count(1) por defecto. Alias un nombre de columna descriptivo que no requerirá escapar de este nombre de columna en más consultas SQL.
(Los paréntesis tampoco son válidos en los nombres de las columnas cuando se escribe en formato parquet).
*/

%sql
CREATE OR REPLACE TEMP VIEW station_counts
AS(
SELECT STATION,unit,TAVG,COUNT(1) AS conteo
FROM weather
GROUP BY STATION,UNIT,TAVG
ORDER BY 1,2,3)



/*
Esta vista agregada es lo suficientemente pequeña para un examen manual. SELECCIONAR * devolverá todos los registros como una vista tabular interactiva.
Tome nota mental de si alguna estación reporta o no la temperatura en ambas unidades, y el número aproximado de registros para cada estación.

Nota al margen Si bien no se activó ningún trabajo al definir la vista, se activa un trabajo cada vez que se ejecuta una consulta en la vista.
*/

%sql
select * from station_counts

/*
Definir un dataframe desde una vista
Volver a transformar una tabla o vista en un DataFrame es simple y no desencadenará un trabajo. Los metadatos asociados con la tabla simplemente se reasignan a DataFrame,
por lo que ahora se puede acceder a los mismos permisos de acceso y esquema a través de las API de DataFrame y SQL.

Use spark.table() para crear un DataFrame llamado countsDF desde la vista "station_counts".
*/

val countsDF = spark.table("station_counts")
countsDF.printSchema()

/*
Escribir a Parquet
Al volver a escribir este DataFrame en el disco, los agregados calculados persistirán para más adelante.

Guarde countsDF en el countsPath provisto en formato parquet.
*/

val countsPath = s"$userhome/stationCounts"

countsDF.write
.format("parquet")
.mode("overwrite")
.save(countsPath)


/*
Sinopsis
En este laboratorio nosotros:

Lea archivos de Parquet en DataFrame con y sin definir un esquema
Guardó una tabla administrada y no administrada
Creó una vista temporal agregada de nuestros datos
Creó un marco de datos a partir de esa vista temporal
Escribió el nuevo marco de datos en los archivos de Parquet
*/

/*
OPCIONAL: Exploración de tablas administradas y no administradas
En esta sección, exploraremos el comportamiento predeterminado de Databricks en tablas administradas y no administradas.
Las diferencias en la sintaxis para definirlos son pequeñas, pero las implicaciones de rendimiento, costo y seguridad pueden ser significativas.
En casi todos los casos de uso, se prefieren las tablas NO administradas.
*/

/*
Guardar una tabla administrada
Para explorar las diferencias entre tablas administradas y no administradas, guarde el WeatherDF de DataFrame de antes en la lección sin la opción "ruta".
Utilice el nombre de la tabla "weather_managed".
*/

weatherDF
  .write
  .mode(SaveMode.Overwrite)
  .saveAsTable("weather")

  /*
Revisar el catálogo de Spark
Tenga en cuenta el campo tableType para nuestras tablas y vistas:

La tabla no administrada weather es EXTERNA
La tabla administrada weather_managed está ADMINISTRADA
La vista temporal station_counts es TEMPORAL
*/

display(spark.catalog.listTables)
/*
name	          |database	              |description	|tableType	|isTemporary
------------------------------------------------------------------------------
iris	          |abel_rivera_teia_mx_db	|null	        |EXTERNAL	  |false
weather	        |abel_rivera_teia_mx_db	|null	        |EXTERNAL	  |false
weather_managed	|abel_rivera_teia_mx_db	|null	        |MANAGED	  |false
station_counts	|null	                  |null	        |TEMPORARY	|true
*/  


/*
El uso de SQL SHOW TABLES proporciona la mayor parte de la misma información, pero no indica si se administra o no una tabla.
*/

%sql

SHOW TABLES

/*
database	              |tableName	      |isTemporary
-------------------------------------------------------
abel_rivera_teia_mx_db	|iris	            |false
abel_rivera_teia_mx_db	|weather	        |false
abel_rivera_teia_mx_db	|weather_managed	|false
	                      |station_counts	  |true
*/

/*
Examinar los detalles de la tabla
Utilice el comando SQL DESCRIBE EXTENDED table_name para examinar las dos tablas meteorológicas.
*/
%sql

DESCRIBE EXTENDED weather

/*
col_name	data_type	comment
NAME	    string	  null
STATION	  string	  null
LATITUDE	float	    null
LONGITUDE	float	    null
ELEVATION	float	    null
DATE	    date	    null
UNIT	    string	  null
TAVG	    float	    null
		
# Detailed Table Information		
Database	        abel_rivera_teia_mx_db	
Table	            weather	
Owner	            root	
Created Time	    Thu Jan 20 01:42:35 UTC 2022	
Last Access	      UNKNOWN	
Created By	      Spark 3.0.1	
Type	            EXTERNAL	
Provider	        parquet	
Table Properties	[bucketing_version=2]	
Location        	dbfs:/user/abel.rivera@teia.mx/weather	
Serde             Library	org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe	
InputFormat     	org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat	
OutputFormat    	org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
*/

%sql

DESCRIBE EXTENDED weather_managed

/*
col_name	data_type	comment
NAME    	string  	null
STATION	  string  	null
LATITUDE	float     null
LONGITUDE	float     null
ELEVATION	float     null
DATE	    date      null
UNIT	    string	  null
TAVG	    float	    null
		
# Detailed Table Information		
Database	        abel_rivera_teia_mx_db	
Table           	weather_managed	
Owner           	root	
Created Time	    Thu Jan 20 01:42:46 UTC 2022	
Last Access	      UNKNOWN	
Created By	      Spark 3.0.1	
Type            	MANAGED	
Provider	        parquet	
Table Properties	[bucketing_version=2]	
Location	        dbfs:/user/hive/warehouse/abel_rivera_teia_mx_db.db/weather_managed	
Serde Library     org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe	
InputFormat       org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat	
OutputFormat      org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat	
*/

/*
Ejecute la siguiente celda para asignar la variable ManagedTablePath y confirme que ambas rutas son correctas con la información impresa arriba.
*/

