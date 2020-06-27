package dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{
  DateType,
  DoubleType,
  LongType,
  StringType,
  StructField,
  StructType
}

object DataSources extends App {

  val spark = SparkSession
    .builder()
    .appName("DataSources")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    ))
// READING A DATAFRAME FROM A FILE SOURCE
//  1. We can also use option("inferSchema", true)
//  but this practice is not recommended in production
// 2. Options - We can either provide option one by one or using map
// In case of the mode option - default is permissive(keeps the malformed one), other valid ones
//  are failFast(raises exception) and dropMalformed(drops the malformed one)
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .options(
      Map(
        "mode" -> "failFast",
        "path" -> "src/main/resources/data/cars.json"
      ))
    .load() //since path is already provided in the options

  carsDF.show(10)

//  WRITING A DATAFRAME TO A FILE
//  Dumps the data from all partitions in to separate files
  //carsDF.write.json("src/main/resources/output/cars/cars1.json")

//  carsDF.write
//    .format("json")
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/output/cars/cars2.json")

  // Specifying a format to parse a field
  // If the date format in the data does not conform to the format
  //  specified, spark populates null
  val newCarSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    ))

  val newCarsDF = spark.read
    .format("json")
    .schema(newCarSchema)
    .option("dateFormat", "YYYY-MM-dd") // better to couple this with the schema
    .option("allowSingleQuotes", true)
    .option("compression", "uncompressed") //spark can automatically uncompress bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

//  CSV parsing
  val stocksSchema = StructType(
    Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    )
  )

  // Documentation of format of various date styles are present here
  // https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html
  val stocksDF = spark.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM d yyyy")
    .option("header", true) // ignores first row in the file which is the column names
    .option("sep", ",") // can be other separator like tab
    .option("nullValue", "") // parse empty data/cell to null
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.show(10)

  //  3. Parquet format
  //  Open source compressed binary data storage optimized for reading columns
  //  default storage format for data frames
//  carsDF.write
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/output/cars/cars.parquet")

//  we can also write as
//  carsDF.write
//    .format("parquet")
//    .mode(SaveMode.Overwrite)
//    .save("src/main/resources/output/cars/cars.parquet")

//  5. Reading from text file
//  one line at a time
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

//  6. Reading from database
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver") // this driver will change for different databases
    .option("url", "jdbc:postgresql://localhost:5433/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show(10)
}
