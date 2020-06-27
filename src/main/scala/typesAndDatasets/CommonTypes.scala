package typesAndDatasets

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object CommonTypes extends App {

//  References for spark sql:
//  https://spark.apache.org/docs/2.4.6/api/sql/index.html
  val spark = SparkSession
    .builder()
    .appName("CommonTypes")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  // insert a column of literal value
  // lit will work with any object like Int, Double, String etc
  moviesDF
    .select(
      col("Title"),
      lit(47).as("PlainValue")
    )
    .show(10)

  //I can either use === or equalTo
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  moviesDF
    .select(
      col("Title")
    )
    .where(dramaFilter)
    .show(10)

  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  // filter can become a column in the data frame
  // That column can also be used in the where clauses
  val dramaMoviesDF = moviesDF
    .select(
      col("Title"),
      preferredFilter.as("GoodDramaMovies")
    )

  //good drama movies
  dramaMoviesDF
    .where(col("GoodDramaMovies")) // boolean value column can be used as it is in the where clause
    .show(10)

  // bad drama movies
  dramaMoviesDF
    .where(not(col("GoodDramaMovies")))
    .show(10)

  // Numbers
  // columns can be used in math expressions
  val rotTomatoAndIMDBRatingCorrelation = (col("Rotten_Tomatoes_Rating") / 10 + col(
    "IMDB_Rating")) / 2

  moviesDF
    .select(
      col("Title"),
      rotTomatoAndIMDBRatingCorrelation.as("AvgRating")
    )
    .show()

  // corr is an action
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  // String operations
  val carsDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/cars.json")

  // string first letter capitalization of every word
  // lower and upper for lowercase and uppercase respectively
  carsDF
    .select(
      initcap(col("Name"))
    )
    .show(10)

  //contains method
  carsDF
    .select(
      col("Name"),
    )
    .where(
      (col("Name") contains "volkswagon") or
        (col("Name") contains "vw"))
    .show

  val regexPattern = "volkswagon|vw"
  // using regex

  val volkswagonCarsDF = carsDF
    .select(
      col("Name")
    )
    .where(regexp_extract(col("Name"), regexPattern, 0) notEqual "")

  volkswagonCarsDF.show

  volkswagonCarsDF
    .select(
      col("Name"),
      regexp_replace(col("Name"), regexPattern, "People's Car")
    )
    .show()
}
