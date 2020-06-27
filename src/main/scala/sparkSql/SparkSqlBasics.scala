package sparkSql

// References:
// https://spark.apache.org/docs/3.0.0/sql-programming-guide.html
// https://spark.apache.org/docs/3.0.0/sql-ref.html
object SparkSqlBasics extends App {

  // a table can be either managed or external
  // managed databases spark maintains data and metadata
  // while in case of external tables, spark is in charge of metadata only.

  // Dropping a managed table, deletes both data and metadata
  // while dropping an external table from spark-sql does not remove
  // the data from the external database

  //creating external table
  // create table flights (origin string, destination string) \
  // using csv options(header true, path "/home/rtjvm/data/flights");

  // Notes on the spark sql basics can be found here
  // src/main/scala/part4sql/SparkShell.scala

  //==============================================================
  // SQL commands that can be used in spark sql shell
  // https://spark.apache.org/docs/3.0.0/sql-ref.html
}
