package typesAndDatasets

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  DateType,
  DoubleType,
  StringType,
  StructField,
  StructType
}

object ComplexTypesExercises extends App {

  val spark = SparkSession
    .builder()
    .appName("ComplexTypesExercises")
    .config("spark.master", "local")
    .getOrCreate()

  // exercise-1: how do we deal with multiple date formats
  // My solution: I can use coalesce method and select the
  // first non null value that works for a particular format

  val moviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  val parsedDate: Column = coalesce(
    to_date(col("Release_Date"), "MMMMM, yyyy"),
    to_date(col("Release_Date"), "dd-MMM-yy"),
    to_date(col("Release_Date"), "yyyy-MM-dd"),
    to_date(col("Release_Date"), "d-MMM-yy")
  )

  val parsedDateMoviesDF = moviesDF
    .select(
      col("Title"),
      parsedDate.as("Parsed_Release_Date")
    )
    .where(col("Release_Date").isNotNull)

  parsedDateMoviesDF.show()

  parsedDateMoviesDF.where(col("Parsed_Release_Date").isNull).show

  // Exercise-2
  //method1
  val stocksSchema = StructType(
    Seq(
      StructField("Symbol", StringType),
      StructField("Date", DateType),
      StructField("Price", DoubleType)
    )
  )

  // Documentation of format of various date styles are present here
  // https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html
  val stocksDF = spark.read
    .schema(stocksSchema)
    .option("header", true)
    .option("sep", ",")
    .option("dateFormat", "MMM d yyyy")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.show()

  // method 2
  val stockDF2 = spark.read
    .option("header", true)
    .option("sep", ",")
    .option("inferSchema", true)
    .csv("src/main/resources/data/stocks.csv")

  stockDF2.printSchema()
  stockDF2
    .withColumn("parsed_date", to_date(col("date"), "MMM d yyyy"))
    .drop(col("date"))
    .withColumnRenamed("parsed_date", "date")
    .show
}
