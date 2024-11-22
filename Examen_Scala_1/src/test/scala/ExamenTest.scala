import utils.TestInit
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{asc, avg, col, lit, udf}

class ExamenTest extends TestInit {

  import spark.implicits._

  "ejercicio1" should "Hacer lo que pide el ejercicio" in {
    val in = Seq(
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

    assert(in.schema.fields.map(_.name).contains("nombre"))
    assert(in.schema.fields.map(_.name).contains("edad"))
    assert(in.schema.fields.map(_.name).contains("calificacion"))

    val estudiantesFiltrados = in.filter($"calificacion" > 8)
    val estudiantesFiltradosCount = estudiantesFiltrados.count()
    assert(estudiantesFiltradosCount == 7)

    val nombresEstudiantes = in.select("nombre", "calificacion").orderBy($"calificacion".desc)
    val estudiantesOrdenados = nombresEstudiantes.collect()
    assert(estudiantesOrdenados(0).getString(0) == "Sofia")
    assert(estudiantesOrdenados(1).getString(0) == "Ana")

    Examen.ejercicio1()
  }

  "ejercicio2" should "Hacer lo que pide el ejercicio" in {
    val parImparUDF = udf((n: Double) => if (n % 2 == 0) "par" else "impar")

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

    val result = rdd.withColumn("parImpar", parImparUDF(col("edad"))).select("nombre", "edad", "parImpar").orderBy(asc("nombre"))

    val expected = Seq(
      ("Alex", 21, "impar"),
      ("Ana", 19, "impar"),
      ("Carla", 22, "par"),
      ("Diego", 23, "impar"),
      ("Elena", 21, "impar"),
      ("Jorge", 23, "impar"),
      ("Juan", 20, "par"),
      ("Lucia", 22, "par"),
      ("Luis", 25, "impar"),
      ("Maria", 23, "impar"),
      ("Marta", 20, "par"),
      ("Pablo", 22, "par"),
      ("Pedro", 24, "par"),
      ("Sergio", 24, "par"),
      ("Sofia", 19, "impar")
    ).toDF("nombre", "edad", "parImpar").orderBy(asc("nombre"))

    // Validar que el resultado es igual al esperado
    assert(result.collect() === expected.collect())

    Examen.ejercicio2()
  }

  "ejercicio3" should "Hacer lo que pide el ejercicio" in {

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

    val tablaUnida = estudiantes2.join(calificaciones, estudiantes2("id") === calificaciones("id_estudiante"))
      .select("id", "nombre", "asignatura", "calificacion")

    val tablaFinal = tablaUnida.groupBy("id", "nombre").agg(avg("calificacion").alias("media_alumno"))
      .orderBy(asc("id"))

    tablaFinal.show()

    val result = tablaFinal.collect()
    result.length shouldEqual 20

    val mediaAlex = result.find(r => r.getAs[Int]("id") == 1).get.getAs[Double]("media_alumno")
    mediaAlex shouldEqual 8.0 +- 0.1

    Examen.ejercicio3()
  }

  "ejercicio4" should "Hacer lo que pide el ejercicio" in {

    val palabras = List(
      "One Piece", "Naruto", "One Piece", "Dragon Ball Z", "One Piece",
      "Bleach", "My Hero Academia", "Naruto", "Demon Slayer", "Naruto", "Naruto", "Tokyo Ghoul",
      "One Punch Man", "One Piece", "One Piece", "Death Note", "Fullmetal Alchemist", "Hunter x Hunter", "Bleach",
      "One Piece", "Neon Genesis Evangelion", "Bleach", "Bleach", "Hunter x Hunter", "Fairy Tail", "Black Clover"
    )

    val palabrasRDD = spark.sparkContext.parallelize(palabras)

    val df = palabrasRDD.toDF("palabra")

    val conteoPalabrasDF = df.groupBy("palabra")
      .agg(functions.count("palabra").alias("conteo"))
      .orderBy(functions.col("conteo").desc)

    val resultado = conteoPalabrasDF.collect()

    assert(resultado(0).getAs[Long]("conteo") == 6)
    assert(resultado(1).getAs[Long]("conteo") == 4)
    assert(resultado(2).getAs[Long]("conteo") == 4)
    assert(resultado(3).getAs[Long]("conteo") == 2)
    assert(resultado(4).getAs[Long]("conteo") == 1)

    conteoPalabrasDF.show()
  }


}
