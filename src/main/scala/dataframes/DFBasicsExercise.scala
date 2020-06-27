package dataframes

import org.apache.spark.sql.types.{
  DoubleType,
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters.seqAsJavaListConverter

object DFBasicsExercise extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark Dataframe Basics exercises")
    .config("spark.master", "local")
    .getOrCreate()

  //  Exercise - 1
  val smartphones = Seq(
    Row("Motorola", "Moto G1", 4.5, 5),
    Row("Motorola", "Moto G2", 5.0, 8),
    Row("Motorola", "Moto G3", 5.5, 12)
  )

  val smartphoneTuples = Seq(
    ("Motorola", "Moto G2", 5.0, 8),
    ("Motorola", "Moto G1", 4.5, 5),
    ("Motorola", "Moto G3", 5.5, 12)
  )

  val smartphonesSchema = StructType(
    Seq(
      StructField("Manufacturer", StringType),
      StructField("Model", StringType),
      StructField("ScreenSize", DoubleType),
      StructField("CameraResolution", IntegerType)
    )
  )

  //    Method 1
  val smartphonesDF = spark.createDataFrame(smartphones.asJava, smartphonesSchema)
  smartphonesDF.printSchema()

  //  From RDD
  val sparkContext = spark.sparkContext
  val smartphonesDFFromRDD =
    spark.createDataFrame(sparkContext.parallelize(smartphones), smartphonesSchema)
  smartphonesDF.printSchema()

  import spark.implicits._
  //  This toDF method does not work on Seq[Row]
  //  this will infer the schema automatically
  val smartphoneTuplesDF =
    smartphoneTuples.toDF("Manufacturer", "Model", "Screen", "Camera")
  smartphonesDF.printSchema()

//  Exercise part-2
//  "Title":"The Land Girls",
//  "US_Gross":146083,
//  "Worldwide_Gross":146083,
//  "US_DVD_Sales":null,
//  "Production_Budget":8000000,
//  "Release_Date":"12-Jun-98",
//  "MPAA_Rating":"R",
//  "Running_Time_min":null,
//  "Distributor":"Gramercy",
//  "Source":null,
//  "Major_Genre":null,
//  "Creative_Type":null,
//  "Director":null,
//  "Rotten_Tomatoes_Rating":null,
//  "IMDB_Rating":6.1,
//  "IMDB_Votes":1071
  val moviesDF = spark.read
    .format("json")
    .option("inferschema", true)
    .load("src/main/resources/data/movies.json")
  println(moviesDF.count())

// Another way to read from a json file
  val moviesDFJson = spark.read
    .option("inferschema", true)
    .json("src/main/resources/data/movies.json")

  moviesDFJson.take(10).foreach(println)
}
