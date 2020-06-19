package typesAndDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession
    .builder()
    .appName("Complex types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  //1. Date parsing
  val moviesWithReleaseDates = moviesDF
    .select(
      col("Title"),
      // yy - 98 is taken as 2098 and 1998
      //to_date returns null on failure to parse to the given format
      to_date(col("Release_Date"), "dd-MMM-yy").as("Release_Date_Parsed")
    )

  moviesWithReleaseDates
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Release_Date_Parsed")) / 365)
    .show()

  moviesWithReleaseDates
    .select("*")
    .where(col("Release_Date_Parsed").isNull)
    .show()

  //date_add and date_sub can add or subtract days from a particular date

  //2. Structures - more like tuple in Scala
  // Storing and processing a tuple in a column
  moviesDF
    .select(
      col("Title"),
      struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
    )
    .select(
      col("Title"),
      col("Profit").getField("US_Gross").as("US_Profit")
    )
    .show()

  // Structures using expressions
  moviesDF
    .selectExpr(
      "Title",
      "(US_Gross, Worldwide_Gross) as Profit"
    )
    .selectExpr("Title", "Profit.US_Gross")
    .show

  //3. Arrays
  moviesDF
    .select(
      col("Title"),
      split(col("Title"), "\\s|,").as("TitleWords")
    )
    .select(
      col("Title"),
      element_at(col("TitleWords"), 1), // position starts from 1
      expr("TitleWords[0]"), // alternatively using expr
      size(col("TitleWords")),
      array_contains(col("TitleWords"), "Love"), // This returns boolean value
      array_position(col("TitleWords"), "Love") // returns the position
      // of the value in the array. Position starts from 1 unlike index which starts from 0
    )
    .show
}
