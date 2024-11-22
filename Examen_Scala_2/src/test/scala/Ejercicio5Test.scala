import org.scalatest.Matchers._
import utils.TestInit
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


class Ejercicio5Test extends TestInit {


  import spark.implicits._

  "ejercicio5" should "Hacer lo que pide el ejercicio" in {

    val filePath = "C://Users//alex-//Documents//BOOTCAMP KEEPCODING//05 Big-Data-Processing//IntelliJ Idea//Examen_Scala_2//src//test//resources//ventas.csv"

    val ventasDF: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    val ingresosDF: DataFrame = ventasDF
      .withColumn("ingreso_total", col("cantidad") * col("precio_unitario"))
      .groupBy("id_producto")
      .agg(sum("ingreso_total").alias("ingreso_total"))

    val resultados = ingresosDF.collect().map { row =>
      val idProducto = row.get(0).toString
      val ingresoTotal = row.getDouble(1)
      (idProducto, ingresoTotal)
    }.toMap

    assert(resultados("108") == 486.0, "El ingreso total para el producto 108 es incorrecto.")
    assert(resultados("101") == 460.0, "El ingreso total para el producto 101 es incorrecto.")

    Ejercicio5.ejercicio5()
  }

  }
