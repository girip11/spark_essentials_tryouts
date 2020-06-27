package sparkSql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSqlPractice extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark Sql practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    // below setting is required to overwrite table data in already existing directory
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", true)
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/cars.json")

  carsDF.printSchema()

  //regular df api
  carsDF
    .select(
      col("Name")
    )
    .where(col("Origin") === "USA")
    .show

  //  We can also write sql queries to achieve the same things
  //  First step to use sql on data frames is to create a temporary view
  carsDF.createOrReplaceTempView("cars")

  // We use """ to write the sql query
  // the result of the sql method is another dataframe
  // Do not end the sql statement with semicolon. It raises ParseException
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where origin = 'USA'
      |""".stripMargin
  )

  americanCarsDF.show

  // we can execute any sql statement inside the sql method
  // We can create database also
  // by default spark creates these databases under the directory
  // called "spark-warehouse" in the running directory
  // If we need to change the location of the directory where these databases
  // and tables should be created, we can specify the location using the spark session's
  // config method and option "spark.sql.warehouse.dir"
  // This directory can be in a cloud storage as well
  spark.sql("""
              |create database rtjvm
              |""".stripMargin)

  // We can use this like a sql shell
  spark.sql("""
              |use rtjvm
              |""".stripMargin)

  spark.sql("""
              |show databases
              |""".stripMargin).show

  // Transfer DB from a regular DB to spark Managed
  // transfer all the tables from postgresql to spark
  // this will create as many managed tables as in postgresqldb

  val dbOptions: scala.collection.Map[String, String] = Map[String, String](
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost:5433/rtjvm",
    "user" -> "docker",
    "password" -> "docker"
  )

  def readTable(tableName: String) =
    spark.read
      .format("jdbc")
      .options(dbOptions +
        ("dbtable" -> s"public.$tableName"))
      .load()

  val tablesInPostgresql =
    List(
      "departments",
      "dept_emp",
      "dept_manager",
      "employees",
      "salaries",
      "titles"
    )

  def transferTables(tableName: String, shouldSave: Boolean = false): Unit = {
    val tableDF = readTable(tableName)
    //Create this so that we can run sql queries on these tables
    tableDF.createOrReplaceTempView(tableName)
    if (shouldSave) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  tablesInPostgresql.foreach(tableName => transferTables(tableName))

  // Now we have all those tables as spark managed tables
  // to read a table as a dataframe use the spark.read.table method
  val employeesDF = spark.read.table("employees")
  employeesDF.show(10)

  //Exercises
  //1. Read the movies DF and store it as a spark table in the rtjvm database
  val moviesDF = spark.read
    .option("inferSchema", true)
    .json("src/main/resources/data/movies.json")

//  moviesDF.write
//    .mode(SaveMode.Overwrite)
//    .saveAsTable("movies")

  //2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000
  // Reference: https://spark.apache.org/docs/3.0.0/sql-ref-functions-builtin.html#examples-3
  spark.sql("describe extended employees").show()

  spark.sql("""
              |select count(emp_no) from employees
              |where hire_date
              |between to_date('1999-01-01') and to_date('2000-01-01')
              |""".stripMargin).show

  //3. Show the average salaries for the employees hired in between those dates
  // grouped by department
  val avgSalaryByDeptDF =
    spark.sql("""
                |select de.dept_no, avg(s.salary) as avg_salary from
                |(select * from employees where hire_date
                |between to_date('1999-01-01') and to_date('2000-01-01')) as e
                |inner join dept_emp as de on e.emp_no = de.emp_no
                |inner join salaries as s on e.emp_no = s.emp_no
                |group by de.dept_no
                |""".stripMargin)

  avgSalaryByDeptDF.orderBy(col("avg_salary").desc).show()

  //4. Show the name of the best-paying department for employees hired in
  // between those dates
  avgSalaryByDeptDF.createOrReplaceTempView("avg_salary_by_dept")
  spark.sql("""
              | select d.dept_name from departments as d
              | inner join avg_salary_by_dept as avg on d.dept_no = avg.dept_no
              | order by avg.avg_salary desc
              | limit 1
              |""".stripMargin).show
}
