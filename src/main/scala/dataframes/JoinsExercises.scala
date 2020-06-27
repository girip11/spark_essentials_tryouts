package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}

object JoinsExercises extends App {

  val spark = SparkSession
    .builder()
    .appName("JoinExercises")
    .config("spark.master", "local")
    .getOrCreate()

  val dbOptions: scala.collection.Map[String, String] = Map[String, String](
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost:5433/rtjvm",
    "user" -> "docker",
    "password" -> "docker"
  )

  val employeesDF = spark.read
    .format("jdbc")
    .options(dbOptions +
      ("dbtable" -> "public.employees"))
    .load()

  val salariesDF = spark.read
    .format("jdbc")
    .options(dbOptions +
      ("dbtable" -> "public.salaries"))
    .load()

  //  1. show all employees and their max salary
  val employeesMaxSalaryDF = salariesDF
    .groupBy("emp_no")
    .agg(
      max(col("salary")).as("max_salary")
    )
    .select(col("emp_no"), col("max_salary"))

  employeesMaxSalaryDF.show()

  employeesDF
    .join(employeesMaxSalaryDF, "emp_no")
    .select("emp_no", "first_name", "last_name", "max_salary")
    .show

//  2. Show all employees who were never managers
  val deptManagersDF = spark.read
    .format("jdbc")
    .options(dbOptions +
      ("dbtable" -> "public.dept_manager"))
    .load()

  employeesDF.join(deptManagersDF, Seq("emp_no"), "anti").show

//  employees who are/were managers
  employeesDF.join(deptManagersDF, Seq("emp_no"), "semi").show

// 3. find the job titles of the best paid 10 employees in the company

  val titlesDF = spark.read
    .format("jdbc")
    .options(dbOptions +
      ("dbtable" -> "public.titles"))
    .load()

  // first find the best paid employees in the company
  //  Take all the employees latest salary
  val bestPaidEmployeesDF = salariesDF
    .join(
      salariesDF
        .groupBy("emp_no")
        .agg(
          max("to_date").as("to_date")
        ),
      Seq("emp_no", "to_date"))
    .orderBy(col("salary").desc)
    .select("emp_no", "salary")
    .limit(10)

  bestPaidEmployeesDF.show

  val employeeCurrentTitleDF = titlesDF
    .join(
      titlesDF
        .groupBy("emp_no")
        .agg(
          max("to_date").as("to_date")
        ),
      Seq("emp_no", "to_date")
    )
    .select("emp_no", "title")

  val employeesAndTitlesDF = employeesDF
    .join(employeeCurrentTitleDF, "emp_no")
    .select("emp_no", "first_name", "last_name", "title")

  bestPaidEmployeesDF
    .join(employeesAndTitlesDF, "emp_no")
    .orderBy(col("salary").desc)
    .show

}
