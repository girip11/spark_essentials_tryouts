package dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSourcesExercises extends App {

// 1.  read movies from the movies.json
  val spark = SparkSession
    .builder()
    .appName("Datasources exercises")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

//  2. write to file with columns, tab separated
  moviesDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header", true)
    .option("sep", "\t")
    .save("src/main/resources/output/movies/movies.tsv")

//  3. as snappy parquet
  moviesDF.write
    .save("src/main/resources/output/movies/movies.parquet")

//  4. write to postgres database into table public.movies
  moviesDF.write
    .format("jdbc")
    .options(
      Map(
        "driver" -> "org.postgresql.Driver",
        "url" -> "jdbc:postgresql://localhost:5433/rtjvm",
        "user" -> "docker",
        "password" -> "docker",
        "dbtable" -> "public.movies"
      ))
    .save()

//  verification
//  val writtenMoviesDF = spark.read
//    .format("jdbc")
//    .options(
//      Map(
//        "driver" -> "org.postgresql.Driver",
//        "url" -> "jdbc:postgresql://localhost:5433/rtjvm",
//        "user" -> "docker",
//        "password" -> "docker",
//        "dbtable" -> "public.movies"
//      ))
//    .load()
//
//  writtenMoviesDF.show(10)
}
