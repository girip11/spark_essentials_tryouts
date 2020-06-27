package typesAndDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, regexp_extract}

object CommonTypesExercises extends App {
  //  References for spark sql:
  //  https://spark.apache.org/docs/2.4.6/api/sql/index.html
  val spark = SparkSession
    .builder()
    .appName("CommonTypes")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/cars.json")

  // Exercise
  def getCarNames: List[String] = List("ford torino", "amc rebel sst")

  //  1. Using isin
  //  but the drawback is it will look for exact match
  carsDF
    .select(col("Name"))
    .where(col("Name").isin(getCarNames: _*))
    .show()

  //  2. Using regex
  carsDF
    .select(col("Name"))
    .where(regexp_extract(col("Name"), getCarNames.map(_.toLowerCase).mkString("|"), 0)
      .notEqual(""))
    .show()

  //  3. Using contains
  val carNameFilters =
    getCarNames.map(car => col("Name") contains car).fold(lit(false))((a, b) => a or b)
  println(carNameFilters)
  carsDF
    .select(col("Name"))
    .where(carNameFilters)
    .show()

  // always remember the expression has to be an sql expression
  val carNameFiltersExpr =
    getCarNames.map(car => s"Name like '%${car.toLowerCase}%'").mkString(" or ")
  println(carNameFiltersExpr)
  carsDF
    .select(col("Name"))
    .where(carNameFiltersExpr)
    .show()

}
