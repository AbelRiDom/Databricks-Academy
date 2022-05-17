/*Transformaciones y Acciones
Objetivos de aprendizaje
Al final de estas lecciones, usted debería ser capaz de:

Describir la diferencia entre ejecución eager y lazy.
Definir e identificar transformaciones.
Definir e identificar acciones.
Describir los fundamentos de cómo funciona Catalyst Optimizer
Discriminar entre transformaciones amplias y estrechas
*/

/*
Pereza por diseño
Fundamental para Apache Spark son las nociones que

Las transformaciones son LAZY
Las acciones son EAGER
El siguiente código condensa la lógica del laboratorio anterior y usa la API de DataFrames para:

Especifique un esquema, formato y fuente de archivo para que se carguen los datos
Seleccionar columnas para AGRUPAR POR
Agregar con un COUNT
Proporcione un nombre de alias para la salida agregada
Especifique una columna para ordenar
Esta celda define una serie de transformaciones. Por definición, esta lógica dará como resultado un DataFrame y no activará ningún trabajo.
*/

val schemaDDL = "NAME STRING, STATION STRING, LATITUDE FLOAT, LONGITUDE FLOAT, ELEVATION FLOAT, DATE DATE, UNIT STRING, TAVG FLOAT"

val sourcePath = "/mnt/training/weather/StationData/stationData.parquet/"

val countsDF = spark.read
  .format("parquet")
  .schema(schemaDDL)
  .load(sourcePath)
  .groupBy("NAME", "UNIT").count()
  .withColumnRenamed("count", "counts")
  .orderBy("NAME")

  /*
  Debido a que la visualización es una acción, se activará un trabajo, ya que la lógica se ejecuta contra los datos especificados para devolver un resultado.
  */
display(countsDF)

/*
NAME	                                      UNIT	counts
BARNABY CALIFORNIA, CA US	                  C	    151
BIG ROCK CALIFORNIA, CA US	                C	    151
BLACK DIAMOND CALIFORNIA, CA US	            C	    151
BRIONES CALIFORNIA, CA US	                  F	    151
CONCORD BUCHANAN FIELD, CA US	              F	    149
HAYWARD AIR TERMINAL, CA US	                F	    149
HOUSTON INTERCONTINENTAL AIRPORT, TX US	    F	    150
HOUSTON WILLIAM P HOBBY AIRPORT, TX US	    C	    150
LAS TRAMPAS CALIFORNIA, CA US	              C	    151
LOS PRIETOS CALIFORNIA, CA US	              F	    151
MERRITT ISLAND FLORIDA, FL US	              C	    151
OAKLAND NORTH CALIFORNIA, CA US	            F	    151
OAKLAND SOUTH CALIFORNIA, CA US	            F	    151
PULGAS CALIFORNIA, CA US	                  F	    151
SAN FRANCISCO INTERNATIONAL AIRPORT, CA US	C	    149
SPRING VALLEY CALIFORNIA, CA US	            F	    151
WOODACRE CALIFORNIA, CA US	                F	    151
*/

/*
¿Por qué es tan importante la pereza?
La pereza es el núcleo de Scala y Spark.

Tiene una serie de beneficios:

No forzado a cargar todos los datos en el paso #1
Técnicamente imposible con conjuntos de datos REALMENTE grandes.
Operaciones más fáciles de paralelizar
Se pueden procesar N transformaciones diferentes en un solo elemento de datos, en un solo hilo, en una sola máquina.
Las optimizaciones se pueden aplicar antes de la compilación del código.
*/

/*
Optimizador de catalizadores
Debido a que nuestra API es declarativa, tenemos a nuestra disposición una gran cantidad de optimizaciones.

Algunos de los ejemplos incluyen:

Optimización del tipo de datos para el almacenamiento
Reescritura de consultas para el rendimiento
Empujes de predicado
*/

/*
Comportamiento
En el código de producción, las acciones generalmente escribirán datos en el almacenamiento persistente usando el DataFrameWriter discutido en los Notebooks anteriores.

Durante el desarrollo de código interactivo en Notebooks de Databricks, el método de visualización se usará con frecuencia para materializar una vista de los datos después de 
que se haya aplicado la lógica.

Una serie de otras acciones brindan la capacidad de devolver vistas previas o especificar planes de ejecución física sobre cómo la lógica se asignará a los datos.
Para ver la lista completa, revise los documentos de la API.

Método                    Devolución     Descripción
collect()                 Collection     Devuelve una matriz que contiene todas las filas de este conjunto de datos.
count()                   Long           Devuelve el número de filas en el conjunto de datos.
first()                   Row            Devuelve la primera fila.
foreach(f)                -              Aplica una función f a todas las filas.
foreachPartition(f):      -              aplica una función f a cada partición de este conjunto de datos.
head()                    Row            Devuelve la primera fila.
reduce(f)                 Row            Reduce los elementos de este conjunto de datos utilizando la función binaria especificada.
show(..)                  -              Muestra las primeras 20 filas de Dataset en forma tabular.
take(n)                   Collection     Devuelve las primeras n filas del conjunto de datos.
toLocalIterator()         iterador       Devuelve un iterador que contiene todas las filas en este conjunto de datos.

Acciones como recopilar pueden provocar errores de falta de memoria al forzar la recopilación de todos los datos.
*/

/*
Transformaciones estrechas
Los datos necesarios para calcular los registros en una sola partición residen como máximo en una partición del RDD padre.

Ejemplos incluyen:
filter(..)
drop(..)
coalesce()
*/
display(countsDF.filter($"NAME".like("%TX%")))
/*
NAME	                                    UNIT	counts
HOUSTON INTERCONTINENTAL AIRPORT, TX US	  F	    150
HOUSTON WILLIAM P HOBBY AIRPORT, TX US	  C	    150
*/
/*
Transformaciones amplias
Los datos necesarios para calcular los registros en una sola partición pueden residir en muchas particiones del RDD principal. 
Estas operaciones requieren que los datos se mezclen entre los ejecutores.

Ejemplos incluyen:
distinct()
groupBy(..).sum()
repartition(n)
*/

display(countsDF.groupBy("UNIT").sum("counts"))

/*
UNIT	sum(counts)
F	    1505
C	    1054
*/

/*
Pipelining
La Pipelining es la idea de ejecutar tantas operaciones como sea posible en una sola partición de datos.
Una vez que una sola partición de datos se lee en la RAM, Spark combinará tantas operaciones estrechas como pueda en una sola tarea.
Las operaciones amplias fuerzan una mezcla, concluyen una etapa y finalizan una Pipelining
*/

/*
Shuffles
Una operación aleatoria(Shuffle) se activa cuando los datos necesitan moverse entre ejecutores.

Para llevar a cabo la operación de reproducción aleatoria, Spark necesita

-Convierta los datos a UnsafeRow, comúnmente conocido como formato binario de tungsteno.
-Escriba esos datos en el disco del nodo local; en este punto, la ranura está libre para la siguiente tarea.
-Envía esos datos a través del wire (cable) a otro ejecutor
      Técnicamente, el controlador decide qué ejecutor obtiene qué dato.
      Luego, el ejecutor extrae los datos que necesita de los archivos aleatorios del otro ejecutor.
-Copie los datos nuevamente en la RAM en el nuevo ejecutor
      El concepto, si no la acción, es como la lectura inicial con la que comienza "cada" DataFrame.
      La principal diferencia es que es la segunda etapa o más.
      The main difference being it's the 2nd+ stage.

Como veremos en un momento, esto equivale a un caché libre de lo que efectivamente son archivos temporales.

Nota al margen Algunas acciones inducen una reproducción aleatoria. Buenos ejemplos incluirían las operaciones count() y reduce(...).

Para obtener más detalles sobre la reproducción aleatoria, consulte la Guía de programación de RDD.
*/

/*
UnsafeRow (también conocido como formato binario de tungsteno)
Como nota al margen rápida, los datos que se "mezclan" están en un formato conocido como UnsafeRow, o más comúnmente, el formato binario de tungsteno.

UnsafeRow es el formato de almacenamiento en memoria para Spark SQL, DataFrames y Datasets.

Las ventajas incluyen:

Compacidad:
Los valores de columna se codifican mediante codificadores personalizados, no como objetos JVM (como ocurre con los RDD).
El beneficio de usar los codificadores personalizados de Spark 2.x es que obtiene casi la misma compacidad que la serialización de Java, pero velocidades de 
codificación/descodificación significativamente más rápidas.
Además, para tipos de datos personalizados, es posible escribir codificadores personalizados desde cero.
Eficiencia: Spark puede operar directamente desde Tungsten, sin deserializar primero los datos de Tungsten en objetos JVM.
Cómo funciona UnsafeRow
El primer campo, "123", se almacena en su lugar como su primitiva.
Los siguientes 2 campos, "data" y "bricks", son cadenas y son de longitud variable.
Se almacena un desplazamiento para estas dos cadenas (32L y 48L, respectivamente, que se muestran en la imagen a continuación).
Los datos almacenados en estos dos desplazamientos tienen el formato "length + data".
En el desplazamiento 32L, almacenamos 4 + "datos" y, del mismo modo, en el desplazamiento 48L almacenamos 6 + "bricks".
*/
/*
Stages
Cuando mezclamos datos, se crea lo que se conoce como un límite de Stage.
Los límites de Stage representan un cuello de botella del proceso.
Tomemos por ejemplo las siguientes transformaciones:

Step	Transformation
1	    Read
2	    Select
3	    Filter
4	    GroupBy
5	    Select
6	    Filter
7	    Write

Spark dividirá este trabajo en dos etapas (pasos 1-4b y pasos 4c-8):

Stage #1

Step	Transformation
1	    Read
2	    Select
3	    Filter
4a    GroupBy 1/2
4b	  shuffle write

Stage #2

Step	Transformation
4c	  shuffle read
4d	  GroupBy 2/2
5	    Select
6	    Filter
7	    Write

En la Etapa n.° 1, Spark creará una canalización de transformaciones en la que los datos se leerán en la RAM (Paso n.° 1) 
y luego realizará los pasos n.° 2, n.° 3, n.° 4a y n.° 4b.

Todas las particiones deben completar la Etapa n.° 1 antes de continuar con la Etapa n.° 2

No es posible agrupar todos los registros en todas las particiones hasta que se complete cada tarea.
Este es el punto en el que todas las tareas deben sincronizarse.
Esto crea nuestro cuello de botella.
Además del cuello de botella, esto también es un impacto significativo en el rendimiento: E/S de disco, E/S de red y más E/S de disco.
Una vez que se mezclan los datos, podemos reanudar la ejecución...

Para la etapa n.° 2, Spark volverá a crear una canalización de transformaciones en la que los datos aleatorios se leen en la RAM (paso n.° 4c) 
y luego realiza las transformaciones n.° 4d, n.° 5, n.° 6 y, finalmente, la acción de escritura, paso n.° 7.
*/

/*
Lineage
Desde la perspectiva del desarrollador, empezamos con una lectura y concluimos (en este caso) con una escritura.

Step	Transformation
1	    Read
2	    Select
3	    Filter
4	    GroupBy
5	    Select
6	    Filter
7	    Write

Sin embargo, Spark comienza con la acción (write(..) en este caso).

A continuación, hace la pregunta, ¿qué debo hacer primero?

Luego procede a determinar qué transformación precede a este paso hasta que identifica la primera transformación.

Step	Transformation	
7    	Write	          Depends on #6
6    	Filter	        Depends on #5
5    	Select	        Depends on #4
4    	GroupBy	        Depends on #3
3    	Filter	        Depends on #2
2    	Select	        Depends on #1
1    	Read	          First
*/

/*
¿Por qué trabajar al revés?
Pregunta: Entonces, ¿cuál es el beneficio de trabajar hacia atrás a través del linaje de su acción?
Respuesta: Permite que Spark determine si es necesario ejecutar cada transformación.

Echa otro vistazo a nuestro ejemplo:

Digamos que ya hemos ejecutado esto una vez
En la primera ejecución, el paso n.º 4 dio como resultado una reproducción aleatoria
Esos archivos aleatorios están en varios ejecutores (src y dst)
Debido a que las transformaciones son inmutables, ningún aspecto de nuestro linaje puede cambiar.
Eso significa que los resultados de nuestra última reproducción aleatoria (si aún están disponibles) se pueden reutilizar.

Step	Transformation	
7     Write	            Depends on #6
6     Filter	          Depends on #5
5     Select	          Depends on #4
4     GroupBy	          <<< shuffle
3     Filter	          don't care
2     Select	          don't care
1     Read	            don't care

En este caso, lo que terminamos ejecutando son solo las operaciones de la Etapa #2.

Esto nos ahorra la lectura inicial de la red y todas las transformaciones en la Etapa #1

Step	Transformation	
1	    Read	          skipped
2	    Select	        skipped
3	    Filter	        skipped
4a	  GroupBy 1/2	    skipped
4b	  shuffle write	  skipped
4c	  shuffle read	  -
4d	  GroupBy 2/2	    -
5	    Select	        -
6	    Filter	        -
7	    Write	          -
*/

/*
Y el almacenamiento en caché...
La reutilización de archivos aleatorios (también conocidos como nuestros archivos temporales) es solo un ejemplo de cómo Spark optimiza las consultas en cualquier 
lugar que pueda.

No podemos asumir que esto estará disponible para nosotros.

Los archivos aleatorios son, por definición, archivos temporales y eventualmente se eliminarán.

Sin embargo, almacenamos datos en caché para lograr explícitamente lo mismo que sucede sin darse cuenta con los archivos aleatorios.

En este caso, el linaje juega el mismo papel. Toma por ejemplo:

Step	Transformation	
7	    Write	          Depends on #6
6	    Filter	        Depends on #5
5	    Select	        <<< cache
4	    GroupBy	        <<< shuffle files
3	    Filter	        ?
2	    Select	        ?
1	    Read	          ?

En este caso, almacenamos en caché el resultado de la select(...).

Ni siquiera llegamos a la parte del linaje que involucra la mezcla, y mucho menos a la Etapa #1.

En cambio, continuamos con el caché y reanudamos la ejecución desde allí:

Step	Transformation	
1	    Read	            skipped
2	    Select	          skipped
3	    Filter	          skipped
4a	  GroupBy 1/2	      skipped
4b	  shuffle write	    skipped
4c	  shuffle read	    skipped
4d	  GroupBy 2/2	      skipped
5a	  cache   read	    -
5b	  Select	          -
6	    Filter	          -
7	    Write	            -
*/


    
