package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  approx_count_distinct,
  avg,
  col,
  count,
  countDistinct,
  max,
  mean,
  min,
  stddev,
  sum
}

object Aggregations extends App {

  val spark = SparkSession
    .builder()
    .appName("Aggregations")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  //  counting
  //  Below count excludes counting null
  val movieGenreCountDF = moviesDF.select(count(col("Major_Genre")))
  movieGenreCountDF.show()

  //  alternatively
  moviesDF.selectExpr("count(Major_Genre)").show()

  //  To include counting null
  moviesDF.select(count("*")).show()

  //  To get the count of distinct values only
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  //  approximate count
  //  can be used on very large datasets to get quick result
  //  without scanning each dataframe
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  // min and max, avg and sum
  //  we can also use selectExpr
  moviesDF.select(min(col("IMDB_Rating"))).show()
  moviesDF.select(max(col("IMDB_Rating"))).show()

  moviesDF.select(sum(col("US_Gross"))).show()
  moviesDF.selectExpr("sum(US_Gross)").show() // alternate way

  moviesDF.select(avg(col("Rotten_Tomatoes_Rating"))).show()

  //  standard deviation and mean
  moviesDF
    .select(
      mean(col("Rotten_Tomatoes_Rating")).as("MEAN"),
      stddev(col("Rotten_Tomatoes_Rating")))
    .as("StandardDev")
    .show()
  // ======================================================================
  // Grouping
  // ======================================================================
  // groupBy is very similar to sql. this should be followed by aggregation

  val movieCountByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .count()

  movieCountByGenreDF.show()

  val avgIMDBRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")
  avgIMDBRatingByGenreDF.show()

  // Using agg method for aggregation
  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("Movies_Per_Genre"),
      avg("IMDB_Rating").as("Avg_IMDB_Rating")
    )
    .orderBy("Avg_IMDB_Rating")

  aggregationsByGenreDF.show()
}
