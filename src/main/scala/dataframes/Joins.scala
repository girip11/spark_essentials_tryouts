package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.functions.{array_contains, expr}

// additional references: https://medium.com/@achilleus/https-medium-com-joins-in-apache-spark-part-2-5b038bc7455b

//Joins are wide transformations and these operations
// invovle shuffles
object Joins extends App {
  val spark = SparkSession
    .builder()
    .appName("Dataframe Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/bands.json")

//  Joins
//  Type of join to perform. Default `inner`. Must be one of:
//   `inner`,
//   `cross`,
//   `outer` `full`  `fullouter` `full_outer`,
//   `left` `leftouter` `left_outer`
//   `right` `rightouter` `right_outer`
//   `semi`, `leftsemi`, `left_semi`
//   `anti`, `leftanti`, left_anti`.

  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")

//  inner join = only row from both left and right that match the join condition
  val guitaristsBandsDF =
    guitaristsDF.join(bandsDF, joinCondition, "inner")
  guitaristsBandsDF.show()

  //  With a join condition, cross also behaves like inner join only
  //  so do the cross join properly, we can use the crossJoin method.
  guitaristsDF.join(bandsDF, joinCondition, "cross").show()

  // left outer join
//  all rows from the left table irrespective of the join condition
//  and matching rows from the right table. Those rows that don't have
//  any matching rows in the right table will have null for all the columns of the
//  right table
  guitaristsDF.join(bandsDF, joinCondition, "left_outer").show

//  similar to left but all rows from the right table are returned
//  while only the matching row columns from the left are taken
  guitaristsDF.join(bandsDF, joinCondition, "right_outer").show

//  full outer - all rows from left and right
//  union (left outer and right  outer)
  guitaristsDF.join(bandsDF, joinCondition, "outer").show

//  left_semi - only the matching rows taken only from the left table
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show

  //  left_anti - only the non matching rows taken only from the left table
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show

  //  cross - cartersian product
  guitaristsDF.crossJoin(bandsDF).show

//  self join also can be done
//  self join is done on hierarchical data
//  the dataframe should be aliased.
// also we will be using the string expressions rather than expressions using col
  val employeesDF = spark
    .createDataFrame(
      Seq(
        (1, "ceo", None),
        (2, "manager1", Some(1)),
        (3, "manager2", Some(1)),
        (101, "Amy", Some(2)),
        (102, "Sam", Some(2)),
        (103, "Aron", Some(3)),
        (104, "Bobby", Some(3)),
        (105, "Jon", Some(3))
      ))
    .toDF("employeeId", "employeeName", "managerId")

  val selfJoinedEmp = employeesDF
    .as("employees")
    .join(employeesDF.as("managers"), expr("managers.employeeId = employees.managerId"))

  selfJoinedEmp
    .select(
      expr("employees.employeeName").as("Employee"),
      expr("managers.employeeName").as("Manager")
    )
    .show

//  Catches
//
//  1. After the inner join, in the guitarists and bands we have id column
//  when we select id, we will get exception by using column name(String) spark does not
//  know which column to select, hence raises exception
//  We can resolve this issue in the following ways

  // 1. By renaming column in the right table
  // When columns to be joined have the same names in both the tables
  // spark adds only one such column to the output table.
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band").show

//2. Renaming to a different column name and using a join expression
  val bandsRenamedDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF
    .join(bandsRenamedDF, guitaristsDF.col("band") === bandsRenamedDF.col("bandId"))
    .show

  //  3 Drop the unnecessary column from of the tables in the output table
  //  suppose we don't need the id column from the guitarists table
  guitaristsBandsDF.drop(bandsDF.col("id")).select("id", "band").show

  // 4. We can use select to explicitly mention the column from the corresponding
  //  tables instead of using the column names
  guitaristsBandsDF
    .select(guitaristsDF.col("name").as("guitarist"), bandsDF.col("name").as("band"))
    .show

  //  2. When column values are arrays(complex types)
  //  for instance, in guitarists , guitar column contains multiple values
  guitaristsDF
    .join(guitarsDF, array_contains(guitaristsDF.col("guitars"), guitarsDF.col("id")))
    .show
//  alternatively using the expr syntax
  guitaristsDF
    .join(
      guitarsDF.withColumnRenamed("id", "guitarId"),
      expr("array_contains(guitars, guitarId)"))
    .show
}
