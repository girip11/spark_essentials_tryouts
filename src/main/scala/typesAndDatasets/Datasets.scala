package typesAndDatasets

import java.sql.Date

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

object Datasets extends App {

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

  val numbersDF = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("src/main/resources/data/numbers.csv")

  // This is a dataframe of rows and columns
  numbersDF.printSchema()

  // with datasets we can filter, map etc with predicates of our choice

  implicit val intEncoder = Encoders.scalaInt

  // The above encode is passed implicilty to the below as method
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // We can pass any predicate in scala that operate with Int
  numbersDS.filter(_ < 100).show()

  // sparksession contains all encoder implicits required under implicits

  // define the custom type as case class
  // column names should exactly match class fields
  // and the column names are case sensitive
  case class Car(
    Name: String,
    Miles_per_Gallon: Option[Double],
    Cylinders: Long,
    Displacement: Double,
    Horsepower: Option[Long],
    Weight_in_lbs: Long,
    Acceleration: Double,
    Year: String, // I am having conversion issues with spark 3.0.0
    Origin: String
  )

  // By default case classes extend Product trait
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Type argument should be any type that extend Product type
  // case classes extend Product type so we can pass the case class type
  //implicit val carEncoder = Encoders.product[Car]

  // But instead of importing these encoders explicitly everytime which is cumbersome
  // we can use the spark session implicits object

  import spark.implicits._

  // encoder automatically obtained from the implicits object
  val carsDS: Dataset[Car] = carsDF.as[Car]

  // now we can operate on the cars dataset like any normal scala collection
  // i.e we can use functions like filter, map, flatMap, fold etc

  // Notice that when we have typecasted from dataframe to dataset
  // we should take care of fields with Null values.
  // Sample exception message if our field contains null for non nullable types
  // as we know types like Double(value types under AnyVal are non nullable)
  // java.lang.NullPointerException: Null value appeared in non-nullable field:
  //- field (class: "scala.Double", name: "Miles_per_Gallon")
  //- root class: "typesAndDatasets.Datasets.Car"
  // If the schema is inferred from a Scala tuple/case class,
  // or a Java bean, please try to use scala.Option[_] or other
  // nullable types (e.g. java.lang.Integer instead of int/scala.Int).
  carsDS.map(car => car.Name.toUpperCase()).show()
  // Exercises
  // 1. Count how many cars we have
  val totalCars = carsDS.count()
  println(totalCars)

  // 2. Count how many cars we have with HP > 140
  println(carsDS.filter(car => car.Horsepower.filter(_ > 140).isDefined).count())

  // 3. Average horsepower for entire dataset
  val totalHorsepower: Long =
    carsDS.map(car => car.Horsepower.getOrElse(0L)).reduce(_ + _)

  println(s"Average HP: ${totalHorsepower / totalCars}")

  // alternatively I can also use the avg function or other functions
  // from sql.functions.object
  // Its valid to use dataframe functions on datasets
  // since DataFrame itself is a type alias of Dataset[Row]
  carsDS.select(avg(col("Horsepower"))).show()
}
