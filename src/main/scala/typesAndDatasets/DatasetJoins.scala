package typesAndDatasets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object DatasetJoins extends App {
  // Dataframes - Dataset[Row]
  // Dataset - distributed collection of custom objects Dataset[CustomType]

  // When to use datasets
  // 1. When we want to maintain the type information
  // 2. we want clear and concise code
  // 3. Filters and transformations are easy to express in Datasets compared to dataframes

  //Avoid datasets when
  // 1. Use dataframes when performance is critical. Because when using datasets,
  // Spark can't optimize transformations, which makes it slower.

  val spark = SparkSession
    .builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  import spark.implicits._

  // spark by default reads numbers as Long
  //{"id":5,"model":"Stratocaster","make":"Fender","guitarType":"Electric"}
  case class Guitar(id: Long, model: String, make: String, guitarType: String)

  //{"id":0,"name":"Jimmy Page","guitars":[0],"band":0}
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)

  //{"id":1,"name":"AC/DC","hometown":"Sydney","year":1973}
  case class Band(id: Long, name: String, hometown: String, year: Long)

  // Joins
  val guitarsDS = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitars.json")
    .as[Guitar]

  val guitarPlayersDS = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers.json")
    .as[GuitarPlayer]

  val bandsDS = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/bands.json")
    .as[Band]

  // guitarists with bands
  // inner join is the default one
  // Notice we get a tuple back after the join
  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] =
    guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"))

  guitarPlayerBandsDS.show()

  // Exercises
  // 1. Join guitarPlayers dataset with guitars dataset
  // when the guitarist uses that guitar

  // solution1
  guitarPlayersDS
    .joinWith(
      guitarsDS,
      size(
        array_intersect(
          guitarPlayersDS.col("guitars"),
          array(guitarsDS.col("id"))
        )) =!= 0,
      "full_outer")
    .show

  // solution 2
  guitarPlayersDS
    .joinWith(
      guitarsDS,
      array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")),
      "full_outer")
    .show

  // solution 3 using expr
  guitarPlayersDS
    .joinWith(
      guitarsDS.withColumnRenamed("id", "guitarId"),
      expr("array_contains(guitars, guitarId)"),
      "full_outer")
    .show

  // Groupby
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // encoder automatically obtained from the implicits object
  val carsDS: Dataset[Datasets.Car] = carsDF.as[Datasets.Car]

  // counts the numbers of cars per origin
  // we will have functions like mapGroups, flatMapGroups, mapValues
  // reduce groups etc on the grouped dataset
  carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  // joins and groups are wide transformations
  // These operations will involve shuffle operations
}
