package sparkRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import scala.io.Source

object SparkRDDBasics extends App {
  //Rdd - Resilient distributed datasets
  // Distributed collection of JVM Objects
  // RDD - first citizens of spark
  // building block of higher level api like data frame

  // partitioning can be controlled
  // caching can be done explicitly

  // Prefer dataframe and dataset apis for common tasks
  // Use rdd when we need to fine tune performance

  val spark = SparkSession
    .builder()
    .appName("Introduction to Rdds")
    .config("spark.master", "local")
    .getOrCreate()

  val sparkContext = spark.sparkContext

  // 1. From an existing collection using parallelize
  // very useful in testing scenarios
  val numbers = 1 to 100000

  val numbersRDD: RDD[Int] = sparkContext.parallelize(numbers)
  println(numbersRDD.count())

  case class StockValue(symbol: String, date: String, price: Double)

  // 2. Reading from a local file and using parallelize
  val stocksFile = Source.fromFile("src/main/resources/data/stocks.csv")
  val stockValues = stocksFile
    .getLines()
    .drop(1)
    .map(stockEntry => {
      val components = stockEntry.split("""\s*,\s*""")
      StockValue(components(0), components(1), components(2).toDouble)
    })
    .toSeq

  val stocksRDD = sparkContext.parallelize(stockValues)
  stocksRDD.take(10).foreach(println)

  // 3. Using sparkContext.textFile api
  // returns RDD of each line
  // We also have another method sparkContext.wholeTextFiles
  // this method returns Map[Strin, String]
  // key is the file name and value is its content
  val stocksRDD2 = sparkContext
    .textFile("src/main/resources/data/stocks.csv")
    .filter(!_.toLowerCase.startsWith("symbol"))
    .map(stockEntry => {
      val components = stockEntry.split("""\s*,\s*""")
      StockValue(components(0), components(1), components(2).toDouble)
    })
  stocksRDD2.take(10).foreach(println)

  // 4. Reading from a DataFrame
  val stocksDF = spark.read
    .option("header", value = true)
    .option("inferSchema", value = true)
    .csv("src/main/resources/data/stocks.csv")

  val stocksRDD3: RDD[Row] = stocksDF.rdd
  stocksRDD3.take(10).foreach(println)

  // 5. From a dataset
  import spark.implicits._
  // For this conversion to work always remember that the
  // case class fields should match the csv header names
  val stocksDS = stocksDF.as[StockValue]

  val stocksRDD4: RDD[StockValue] = stocksDS.rdd
  stocksRDD4.take(10).foreach(println)

  // RDD to DataFrame
  val numbersDF1 = numbersRDD.toDF("numbers")
  println(
    numbersDF1
      .select(col("numbers"))
      .where("numbers > 50000")
      .count())

  // we can also use createDataFrame
  // If we are using a type that does not extend Product,
  // we have to convert to a type that extends Product
  // aS we know all case classes extend Product
  case class IntValue(value: Int)
  val numbersDF2 = spark.createDataFrame(numbersRDD.map(IntValue)).toDF("numbers")
  println(numbersDF2.count())

  // RDD to Dataset
  val numbersDS1 = numbersRDD.toDS()
  println(numbersDS1.count())

  val numbersDS2 = spark.createDataset(numbersRDD)
  println(numbersDS2.count())

  // RDD vs datasets(DataFrame is actually Dataset[Row])
  // ======================================================
  // Common
  // Both have map, flatMap, reduce, filter,take etc operations
  // Union, count and distinct
  // groupBy and sortBy
  // RDDs over datasets
  // 1. partition control: repartition coalesce, partitioner, zipPartitions, mapPartitions
  // 2. operation control: checkpoint, isCheckpointed, localCheckpoint, cache
  // 3. storage control: cache, getStorageLevel, persist

  // Datasets over RDDs
  // 1. select and joins are present in datasets
  // 2. spark can plan and optimize before running code

  //==============================================================================
  // Transformations and actions
  // actions trigger computations

  //count and distinct
  val msftStocksRDD = stocksRDD2.filter(_.symbol == "MSFT")
  println(msftStocksRDD.count())

  val companyNamesRDD = stocksRDD2.map(_.symbol).distinct()
  companyNamesRDD.foreach(println)

  // min and max
  // we need to specify the ordering
  implicit val stockOrdering: Ordering[StockValue] =
    Ordering.fromLessThan[StockValue]((sa, sb) => sa.price < sb.price)
  // min expects ordering as implicit parameter
  println(msftStocksRDD.min())

  // reduce and groupBy
  println(numbersRDD.reduce(_ + _))

  // groupBy involves shuffling. Hence it is an expensive operation
  val maxValuePerStock = stocksRDD2.groupBy(_.symbol).mapValues(_.max)
  maxValuePerStock.foreach(println)

  //==========================================================================

  // partitioning
  println(stocksRDD2.partitions.length)

  // Repartition is expensive. So if required we need to do it as
  // earlier as possible
  // Optimal size of partition is between 10 and 100MB
  val repartitionedStocksRDD = stocksRDD2.repartition(30)
//  repartitionedStocksRDD.saveAsTextFile("src/main/resources/data/stocks30")
  println(repartitionedStocksRDD.partitions.length)

  // coalesce to reduce the number of partitions
  // By default shuffle is false. Tries to merge the partitions on the same node
  // to achieve the input number
  val coalescedRDD = repartitionedStocksRDD.coalesce(15)
//  coalescedRDD.saveAsTextFile("src/main/resources/data/stocks15")
  println(coalescedRDD.partitions.length)

  // Exercises
  // 1. movies.json as RDD
  case class Movie(title: String, genre: String, rating: Double)

  // {"Title":"Def-Con 4","US_Gross":210904,"Worldwide_Gross":210904,"US_DVD_Sales":null,"Production_Budget":1300000,"Release_Date":"15-Mar-85","MPAA_Rating":null,"Running_Time_min":null,"Distributor":"New World","Source":"Original Screenplay","Major_Genre":"Action","Creative_Type":"Science Fiction","Director":null,"Rotten_Tomatoes_Rating":null,"IMDB_Rating":3.8,"IMDB_Votes":639}
  //1. Solution using whole text files
  // I could also have used a json serializer instead of regex
  val moviePattern =
    """\{\"Title\"\s*:\s*(null|.*),\"US_Gross\".*\"Major_Genre\"\s*:\s*(null|".*?").*\"IMDB_Rating\"\s*:\s*(null|[\d.]+),?.*\}""".r
  val moviesRDD = sparkContext
    .textFile("src/main/resources/data/movies.json")
    .map(line => moviePattern.findFirstMatchIn(line).map(_.subgroups))
    .filter(m => m.isDefined && m.get.length == 3)
    .map(groups => {
      val title = if (groups.get(0) == "null") null else groups.get(0).replace("\"", "")
      val genre = if (groups.get(1) == "null") null else groups.get(1).replace("\"", "")
      val rating = if (groups.get(2) == "null") 0.0 else groups.get(2).toDouble
      Movie(title, genre, rating)
    })
    .filter(m => m.title != null && m.genre != null && m.rating != 0.0)

  //2. Solution 2 using DataFrame
  import spark.implicits._
  val moviesRDD2 = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")
    .select(
      col("Title").as("title"),
      col("Major_Genre").as("genre"),
      col("IMDB_Rating").as("rating"))
    .where(col("title").isNotNull and col("genre").isNotNull and col("rating").isNotNull)
    .as[Movie]
    .rdd

  moviesRDD.take(10).foreach(println)

  // Both contain same data
  // But it was easier to construct the RDD from the dataframe/dataset
  println(moviesRDD2.count(), moviesRDD.count())

  //2. Distinct Genres
  println(moviesRDD2.map(_.genre).filter(_ != null).distinct())

  //3. All movies in Drama Genre with IMDB rating > 6
  val goodDramaMoviesRDD = moviesRDD2
    .filter(m => m.genre == "Drama" && m.rating > 6)
    .map(_.title)

  println(goodDramaMoviesRDD.count())

  //4. Show the average rating of movies by genre
  val averageRatingMoviesRDD = moviesRDD2
    .filter(_.genre != null)
    .groupBy(_.genre)
    .mapValues(movies => movies.map(_.rating).sum / movies.size)

  averageRatingMoviesRDD.foreach(println)
}
