package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {
  val spark = SparkSession
    .builder()
    .appName("Columns and expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/cars.json")

  carsDF.show(10)

  // ========================================================================
  // selecting columns - This is called projection
  val nameColumn = carsDF.col("Name")
  val carNamesDF = carsDF.select(nameColumn)
  carNamesDF.show(10)

  //  various ways in which we can select columns
  //  1. using functions.col, functions.column, functions.expr helpers
  //  and using dataframe col method
  val carDetailsDF = carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Origin"),
    expr("Weight_in_lbs")
  )

  //  2. Using implicits from sparkSession object
  import spark.implicits._

  val carAccelerationDetailsDF = carsDF.select(
    'Name,
    $"Acceleration"
  )

  //  3.Using just the column names
  val carsOriginDF = carsDF.select("Name", "Origin")

  // ========================================================================
  //  EXPRESSIONS
  // ========================================================================
  // 1. Expressions as scala code
  val simpleExpr = carsDF.col("Weight_in_lbs") / 2.2
  val carsWeightInKgDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    simpleExpr.as("Weight_in_Kgs")
  )

  carsWeightInKgDF.show()

  //  2. Expressions as string to expr method
  val carsWeightInKgDF2 = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_Kgs")
  )

  carsWeightInKgDF2.show()

  //  Expression strings passed to selectExpr method
  val carsWeightInKgDFExpr = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "(Weight_in_lbs / 2.2) as Weight_in_Kgs"
  )

  carsWeightInKgDFExpr.show()

  // Adding new columns to existing
  // dataframe results in new DF with added columns
  val carWeightsInKgDFNewCol =
    carsDF.withColumn("Weight_in_Kgs", col("Weight_in_lbs") / 2.2)

  // Renaming columns
  val carsWeightRenamedDF = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in Pounds")
  //  column names with spaces require backticks in string expressions
  carsWeightRenamedDF.selectExpr("`Weight in Pounds`").show(10)

  //  Deleting a column from a dataframe
  carsWeightRenamedDF.drop("Acceleration", "Displacement").show(10)

  // ========================================================================
  //  Filtering using filter and where methods
  // ========================================================================

  val europeanOriginCarsDF =
    carsDF.filter(col("Origin") === "Europe")
  europeanOriginCarsDF.show(10)

  //  All except Japanese cars
  carsDF.where(col("Origin") =!= "Japan").count()

  //  filter using string expressions
  carsDF.filter("Origin = 'USA'").count()

  //  American powerful cars
  carsDF.filter("Origin = 'USA' and Horsepower > 150").count()
  carsDF
    .filter(
      col("Origin") === "USA" and
        col("Horsepower") > 150)
    .count()

  // Union of dataframes
  // To do union of dataframes, those dataframes
  // should be having the same schema
  val moreCarsDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCarsDF)
  allCarsDF.count()

  //  distinct origins of cars
  val carUniqueOriginsDF = carsDF.select("Origin").distinct()
  carUniqueOriginsDF.show()
}
