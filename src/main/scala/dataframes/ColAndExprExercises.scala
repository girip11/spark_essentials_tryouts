package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, column, expr, isnull, not, when}

object ColAndExprExercises extends App {

  val spark = SparkSession
    .builder()
    .appName("ColsAndExprExercises")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", value = true)
    .json("src/main/resources/data/movies.json")

  moviesDF.printSchema()

//  1. select any 2 columns of your choice
  val movieNamesAndImdbRatingDF = moviesDF.select("Title", "IMDB_Rating")
  val movieNamesAndImdbRatingDF2 =
    moviesDF.select(moviesDF.col("Title"), moviesDF.col("IMDB_Rating"))

//  This syntax requires spark.implicits to be imported
  import spark.implicits._
  val movieNamesAndImdbRatingDF3 =
    moviesDF.select('Title, expr("IMDB_Rating"), $"Release_Date")

  val movieNamesAndImdbRatingDF4 =
    moviesDF.select(col("Title"), column("IMDB_Rating"))

  movieNamesAndImdbRatingDF.show(10)
//  movieNamesAndImdbRatingDF2.show(10)
//  movieNamesAndImdbRatingDF3.show(10)
//  movieNamesAndImdbRatingDF4.show(10)

//  2. Create another column sum = UDGross + worldwide_gross + dvdsales
//  US_Gross":36690067,"Worldwide_Gross":36690067,"US_DVD_Sales":null

//  Simpler versions omit DVD Sales
  val movieEarningsSimplerDF = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as `Total Sales`" // backticks for spaces in column names
  )

  movieEarningsSimplerDF.show(10)

  val movieEarningsSimplerDF2 = moviesDF
    .selectExpr(
      "Title",
      "US_Gross",
      "Worldwide_Gross",
    )
    .withColumn("Total_Sales", col("US_Gross") + col("Worldwide_Gross"))

  movieEarningsSimplerDF2.show(10)

  val movieEarningExpr = when(isnull(col("US_Gross")), 0.0)
    .otherwise(col("US_Gross")) +
    when(isnull(col("Worldwide_Gross")), 0.0)
      .otherwise(col("Worldwide_Gross")) +
    when(isnull(col("US_DVD_Sales")), 0.0)
      .otherwise(col("US_DVD_Sales"))

  val movieEarningsDF = moviesDF
    .select(
      col("Title"),
      col("US_Gross"),
      col("Worldwide_Gross"),
      col("US_DVD_Sales"),
      movieEarningExpr.as("Total_Sales")
    )

  movieEarningsDF.show(10)
  println(movieEarningsDF.count())

  val movieEarningExpr2 =
    coalesce(col("US_Gross"), when(isnull(col("US_Gross")), 0.0)) +
      coalesce(col("Worldwide_Gross"), when(isnull(col("Worldwide_Gross")), 0.0)) +
      coalesce(col("US_DVD_Sales"), when(isnull(col("US_DVD_Sales")), 0.0))

  val movieEarningsDF2 = moviesDF
    .select(
      col("Title"),
      col("US_Gross"),
      col("Worldwide_Gross"),
      col("US_DVD_Sales"),
      movieEarningExpr2.as("Total_Sales")
    )

  movieEarningsDF2.show(10)
  println(movieEarningsDF2.count())
  // 3. Select all comedy movies with IMDB rating above 6
//  "Major_Genre":"Musical","IMDB_Rating":7.8
//  instead of filter, I can also use where
  val goodComedyMoviesDF = moviesDF
    .filter(
      "Major_Genre = 'Comedy' " +
        "and IMDB_Rating is not null " +
        "and IMDB_Rating >= 6.0")
    .select($"Title", expr("Major_Genre"), 'IMDB_Rating)

  print(goodComedyMoviesDF.count())
  goodComedyMoviesDF.show(10)

  val goodComedyMoviesDF2 = moviesDF
    .where(
      col("Major_Genre") === "Comedy"
        and not(isnull(col("IMDB_Rating")))
        and col("IMDB_Rating") >= 6.0)
    .select($"Title", expr("Major_Genre"), 'IMDB_Rating)

  print(goodComedyMoviesDF2.count())
}
