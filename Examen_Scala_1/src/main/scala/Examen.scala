import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

object Examen {

  val spark = SparkSession.builder()
    .appName("Examen_Scala_1")
    .master("local[*]")
    .getOrCreate()

  /** Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   * Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   * estudiantes (nombre, edad, calificación).
   * Realiza las siguientes operaciones:
   *
   * Muestra el esquema del DataFrame.
   * Filtra los estudiantes con una calificación mayor a 8.
   * Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
  def ejercicio1() = {
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.types._
    import spark.implicits._
    val rdd = Seq(
      ("Alex", 21, 8.5),
      ("Maria", 23, 9.2),
      ("Juan", 20, 7.8),
      ("Lucia", 22, 8.0),
      ("Pedro", 24, 6.9),
      ("Ana", 19, 9.5),
      ("Luis", 25, 7.3),
      ("Carla", 22, 8.4),
      ("Jorge", 23, 7.7),
      ("Elena", 21, 8.1),
      ("Sergio", 24, 6.8),
      ("Marta", 20, 9.0),
      ("Pablo", 22, 7.5),
      ("Sofia", 19, 9.6),
      ("Diego", 23, 7.9)
    ).toDF("nombre", "edad", "calificacion")

    val schema = StructType(Seq(
      StructField("nombre", StringType, true),
      StructField("edad", IntegerType, true),
      StructField("calificacion", DoubleType, true)
    ))

    val estudiantes = spark.createDataFrame(rdd.rdd, schema)

    estudiantes.show()
    println(estudiantes.schema)

    val estudiantesFiltrados = estudiantes.filter(col("calificacion") > 8)
    estudiantesFiltrados.show()

    val nombresEstudiantes = estudiantes.select("nombre", "calificacion").orderBy(col("calificacion").desc)
    nombresEstudiantes.show()


  }

  /** Ejercicio 2: UDF (User Defined Function)
   * Pregunta: Define una función que determine si un número es par o impar.
   * Aplica esta función a una columna de un DataFrame que contenga una lista de números. */

  def ejercicio2(): Unit = {
    import spark.implicits._

    val parImparUDF = udf ((n: Double) => if (n % 2 == 0) "par" else "impar")


    val rdd = Seq (
      ("Alex", 21, 8.5),
      ("Maria", 23, 9.2),
      ("Juan", 20, 7.8),
      ("Lucia", 22, 8.0),
      ("Pedro", 24, 6.9),
      ("Ana", 19, 9.5),
      ("Luis", 25, 7.3),
      ("Carla", 22, 8.4),
      ("Jorge", 23, 7.7),
      ("Elena", 21, 8.1),
      ("Sergio", 24, 6.8),
      ("Marta", 20, 9.0),
      ("Pablo", 22, 7.5),
      ("Sofia", 19, 9.6),
      ("Diego", 23, 7.9)
    ).toDF ("nombre", "edad", "calificacion")

    val result = rdd.withColumn("parImpar", parImparUDF (col ("edad"))).select ("nombre", "edad", "parImpar").orderBy(asc("nombre"))
    result.show()
  }



  /**Ejercicio 3: Joins y agregaciones
   Pregunta: Dado dos DataFrames,
   uno con información de estudiantes (id, nombre)
   y otro con calificaciones (id_estudiante, asignatura, calificacion),
   realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */
  def ejercicio3(): Unit = {
    val sc = spark.sparkContext
    import spark.implicits._

    val estudiantes2 = sc.parallelize(Seq(
      (1, "Alex"),
      (2, "Maria"),
      (3, "Juan"),
      (4, "Lucia"),
      (5, "Pedro"),
      (6, "Ana"),
      (7, "Luis"),
      (8, "Carla"),
      (9, "Jorge"),
      (10, "Elena"),
      (11, "Sergio"),
      (12, "Marta"),
      (13, "Pablo"),
      (14, "Sofia"),
      (15, "Diego"),
      (16, "Laura"),
      (17, "Carmen"),
      (18, "Manuel"),
      (19, "Raquel"),
      (20, "Fernando")
    )).toDF("id", "nombre")

    val calificaciones = sc.parallelize(Seq(
      (1, "Matemáticas", 8.5),
      (1, "Física", 7.5),
      (2, "Matemáticas", 9.0),
      (2, "Física", 8.8),
      (3, "Matemáticas", 7.5),
      (3, "Física", 6.9),
      (4, "Matemáticas", 8.2),
      (4, "Física", 8.0),
      (5, "Matemáticas", 6.8),
      (5, "Física", 6.5),
      (6, "Matemáticas", 9.3),
      (6, "Física", 9.0),
      (7, "Matemáticas", 7.0),
      (7, "Física", 7.2),
      (8, "Matemáticas", 8.4),
      (8, "Física", 8.3),
      (9, "Matemáticas", 7.9),
      (9, "Física", 7.7),
      (10, "Matemáticas", 8.7),
      (10, "Física", 8.1),
      (11, "Matemáticas", 6.8),
      (11, "Física", 6.5),
      (12, "Matemáticas", 7.5),
      (12, "Física", 7.8),
      (13, "Matemáticas", 8.9),
      (13, "Física", 8.4),
      (14, "Matemáticas", 9.4),
      (14, "Física", 9.3),
      (15, "Matemáticas", 7.8),
      (15, "Física", 7.6),
      (16, "Matemáticas", 8.1),
      (16, "Física", 8.0),
      (17, "Matemáticas", 6.7),
      (17, "Física", 6.9),
      (18, "Matemáticas", 7.2),
      (18, "Física", 7.1),
      (19, "Matemáticas", 8.3),
      (19, "Física", 8.2),
      (20, "Matemáticas", 9.1),
      (20, "Física", 9.0)
    )).toDF("id_estudiante", "asignatura", "calificacion")

    estudiantes2.show()
    calificaciones.show()

    val tablaUnida = estudiantes2.join(calificaciones, estudiantes2("id") === calificaciones("id_estudiante")).select("id", "nombre", "asignatura", "calificacion")
    tablaUnida.show()

    val tablaFinal = tablaUnida.groupBy("id","nombre").agg(avg("calificacion").alias("media_alumno")).orderBy(asc("id"))
    tablaFinal.show()


  }

   /**Ejercicio 4: Uso de RDDs
   Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.

   */

   def ejercicio4() = {
     val sc = spark.sparkContext

     val palabras = List(
       "One Piece", "Naruto", "One Piece", "Dragon Ball Z", "One Piece",
       "Bleach", "My Hero Academia", "Naruto", "Demon Slayer", "Naruto", "Naruto", "Tokyo Ghoul",
       "One Punch Man", "One Piece", "One Piece", "Death Note", "Fullmetal Alchemist", "Hunter x Hunter", "Bleach",
       "One Piece", "Neon Genesis Evangelion", "Bleach", "Bleach", "Hunter x Hunter", "Fairy Tail", "Black Clover"
     )

     val palabrasRDD = sc.parallelize(palabras)

     import spark.implicits._
     val df = palabrasRDD.toDF("palabra")

     val conteoPalabrasDF = df.groupBy("palabra")
       .agg(functions.count("palabra").alias("conteo"))
       .orderBy(functions.col("conteo").desc)

     conteoPalabrasDF.show()
   }

}
