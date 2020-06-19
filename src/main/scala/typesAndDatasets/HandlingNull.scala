package typesAndDatasets

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object HandlingNull extends App {
  // set the nullable flag to false in StructField only when we are sure the
  // data is free is of null, otherwise leave it as the default

  val spark = SparkSession
    .builder()
    .appName("HandlingNulls")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

  // movies title and either Rotten tomato or IMDB whichever is non null
  moviesDF
    .select(
      col("Title"),
      col("Rotten_Tomatoes_Rating"),
      col("IMDB_Rating"),
      //ordering matters. Selects first non null value.
      coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
        .as("Coalesced_Rating")
    )
    .show

  //checking for nulls using column methods
  moviesDF
    .select("*")
    .where(col("Rotten_Tomatoes_Rating").isNull)

  // null when ordering data
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  // removing nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop() // remove rows containing nulls

  //na object on the data frame has a lot of functions
  // we can replace null with default values
  moviesDF.na.fill(0.0, List("IMDB_Rating", "Rotten_Tomatoes_Rating")).show()
  // replaces null with configured defaults
  moviesDF.na.fill(
    Map(
      "IMDB_Rating" -> 0,
      "Rotten_Tomatoes_Rating" -> 10,
      "Director" -> "Unknown"
    ))

  //complex operations using sql
  // the below operations are not available as it is as dataframe functions
  moviesDF
    .selectExpr(
      "Title",
      "Rotten_Tomatoes_Rating",
      "IMDB_Rating",
      "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", //same as coalesce
      "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", //same as coalesce
      "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif",
      // returns null if (the two values are Equal or first is null) else the first value
      "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) second else third
      // same as ternary operator
    )
    .show

  // This returns 10
  // if (first != second) first else null
  moviesDF.selectExpr("nullif(10, null) as nullif").show(10)

  // This returns null because the first operand is null
  moviesDF.selectExpr("nullif(null, null) as nullif").show(10)
}
