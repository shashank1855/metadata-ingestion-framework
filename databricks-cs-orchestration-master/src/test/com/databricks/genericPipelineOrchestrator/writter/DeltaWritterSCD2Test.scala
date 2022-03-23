package com.databricks.genericPipelineOrchestrator.writter

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant.DELETE
import com.databricks.genericPipelineOrchestrator.utility.Crypt.{decrypt, encrypt}
import com.databricks.genericPipelineOrchestrator.utility.Utility.{decryptUDF, encryptIt, encryptUDF, writeSCD2}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{coalesce, col, lit, udf, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.reflect.io.Directory

class DeltaWritterSCD2Test extends AnyFunSuite with BeforeAndAfterAll {
  @transient var spark: SparkSession = null
  @transient var deltaDir: Directory = null
  @transient val employee_details = "/tmp/delta/test/employee_details"
  @transient val employee_details_table = "test_bronze.employee_details"
  private val encryptUDF: UserDefinedFunction = udf(encrypt _)
  private val decryptUDF: UserDefinedFunction = udf(decrypt _)

  def encryptItTest(df: DataFrame, cols: String*): DataFrame = {
    val key = "123456789"
    val saltKey = "1234$%&(@(&"
    val df1 = df.withColumn("key", lit(key)).withColumn("saltKey", lit(saltKey))

    val newDf = cols.foldLeft(df1) {
      case (tmpdf, columnName) => tmpdf.withColumn(columnName, encryptUDF(col("key"), col("saltKey"), coalesce(col(columnName), lit("n/a")).cast("string")))
    }
    newDf.drop("key").drop("saltKey")

  }

  def decryptItTest(df: DataFrame, cols: String*): DataFrame = {
    val key = "123456789"
    val saltKey = "1234$%&(@(&"
    val df1 = df.withColumn("key", lit(key))
      .withColumn("saltKey", lit(saltKey))

    val newDf = cols.foldLeft(df1) {
      case (tmpdf, columnName) => tmpdf.withColumn(columnName, decryptUDF(col("key"), col("saltKey"), coalesce(col(columnName), lit("n/a")).cast("string")))
    }
    val finalDf = cols.foldLeft(newDf) {
      case (tmpdf, columnName) => tmpdf.withColumn(columnName, when(col(columnName).equalTo("n/a"), null).otherwise(col(columnName)))
    }
    finalDf.drop("key").drop("saltKey")
  }

  def createIntialBronzeTable(): Unit = {

    spark.sql(s"drop table if exists $employee_details_table")
    spark.sql(
      s"""
         |create table $employee_details_table (
         |empid int, ename string, sal double, deptno int,comm int, place string,currentFlag boolean,oc string, effDate timestamp, expryDate timestamp)
         |using delta
         |partitioned by (empid)
         |""".stripMargin)
    val empdf = spark.read.option("header", value = false).schema("empid int, ename string, sal double, deptno int,comm int,place string,currentFlag boolean,oc string, effDate timestamp, expryDate timestamp").csv("src/test/resources/emp_scd2.csv")
    val pd = encryptItTest(empdf, "place")
    pd.write.format("delta").mode("append").partitionBy("empid").saveAsTable(employee_details_table)
  }

  override protected def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    deltaDir = Directory(new File("/tmp/delta/test"))
    deltaDir.deleteRecursively()

    spark.sql("CREATE DATABASE test_bronze LOCATION '/tmp/delta/test/bronze'")
    spark.sql("CREATE DATABASE test_silver LOCATION '/tmp/delta/test/silver'")
    createIntialBronzeTable()
  }

  override protected def afterAll(): Unit = {
    deltaDir.deleteRecursively()
    spark.stop()
  }

  test("1") {

    val df = spark.read.table(employee_details_table)
    decryptItTest(df, "place").orderBy("empid", "currentFlag", "place").show(false)
    val empUpdateDF = spark.read.option("header", value = false).schema("empid int, ename string, sal double, deptno int,comm int,place string,oc string, effDate timestamp").csv("src/test/resources/emp_scd2_update.csv")
    val pd = encryptItTest(empUpdateDF, "place")
    decryptItTest(pd, "place").orderBy("empid", "place").show(false)

    writeSCD2(spark
      , pd
      , employee_details_table
      , Seq("empid = empid", "deptno = deptno")
      , MergeBuilderLogicSCD2(Map("target.currentFlag" -> "false", "target.expryDate" -> "updates.effDate")
        , Some(Map("target.empid" -> "updates.empid"
          , "target.ename" -> "updates.ename"
          , "target.sal" -> "updates.sal"
          , "target.deptno" -> "updates.deptno"
          , "target.comm" -> "updates.comm"
          , "target.place" -> "updates.place"
          , "target.currentFlag" -> "true"
          , "target.effDate" -> "updates.effDate"
          , "target.expryDate" -> "null"))
        , "target.currentFlag = true and target.place <> updates.place")
      , Some("1 = 1 and 2 = 2")
      , Some(Seq("empid"))
      , Some(Seq("effDate")))

    //df.orderBy("empid","currentFlag","place").show(false)
    decryptItTest(df, "place").orderBy("empid", "currentFlag", "place").show(false)

  }

}
