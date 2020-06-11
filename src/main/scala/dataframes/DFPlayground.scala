package dataframes

import org.apache.spark.sql.SparkSession

object DFPlayground extends App {

  val spark = SparkSession
    .builder()
    .appName("Dataframes basics")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .format("json")
    .json("src/main/resources/data/cars.json")

  carsDF.show(10)
  println(carsDF.count())
  carsDF.take(5).foreach(println)

}
