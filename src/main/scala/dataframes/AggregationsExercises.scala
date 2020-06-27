package dataframes

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{
  avg,
  coalesce,
  col,
  countDistinct,
  isnull,
  mean,
  stddev,
  sum,
  when
}

object AggregationsExercises extends App {

  val spark = SparkSession
    .builder()
    .appName("Aggregations")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  //  1. Sum up all the profits of all the movies in the DF
  val totalProfitsDF = moviesDF
    .select(
      sum("US_Gross").as("Net_US_Gross"),
      sum("Worldwide_Gross").as("Net_Worldwide_Gross"),
      sum("US_DVD_Sales").as("Net_US_DVD_Sales")
    )
    .selectExpr("Net_US_Gross + Net_Worldwide_Gross + Net_US_DVD_Sales")

  totalProfitsDF.show()

  def get_column(colName: String): Column =
    coalesce(col(colName), when(isnull(col(colName)), 0))

  //  Alternate way of doing it
  val profitColumn = get_column("US_Gross") +
    get_column("Worldwide_Gross") +
    get_column("US_DVD_Sales")
  val totalProfitsDF2 = moviesDF
    .select(profitColumn.as("Profit"))
    .select(
      sum("Profit").as("TotalProfit")
    )

  totalProfitsDF2.show()

  //  2. distinct directors count of all the movies
  val distinctDirectorCountDF =
    moviesDF.select(countDistinct("Director").as("DistinctDirectorCount"))

  distinctDirectorCountDF.show()

//  3. mean and standard deviation of US gross revenue for movies
  val meanStandardDevGrossRevenueDF = moviesDF
    .select(
      mean("US_Gross").as("Mean_US_Gross"),
      stddev("US_Gross").as("SD_US_Gross")
    )
  meanStandardDevGrossRevenueDF.show()

// 4. Average IMDB Rating and the total US gross revenue per director
//  sort the result by the either rating or revenue

  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_IMDB_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Avg_IMDB_Rating").desc_nulls_last)
//    .sort(
//      col("Avg_IMDB_Rating").desc
//    )
    .show()
}
