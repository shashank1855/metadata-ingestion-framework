package com.databricks.genericPipelineOrchestrator.writter

import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant._
import com.databricks.genericPipelineOrchestrator.utility.Utility.{writeSCD1, writeStreamSCD1}
import com.databricks.genericPipelineOrchestrator.writter.config.WriteStreamConfig
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import java.io.File
import scala.reflect.io.Directory

class DeltaWritterTest extends AnyFunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = null
  @transient var deltaDir: Directory = null
  @transient var deltaSilverDir: Directory = null
  @transient val inputPathForEachBatch = "/tmp/delta/test/tmp"
  @transient val inputPathAppend = "/tmp/delta/test/append"
  @transient val checkPointLoc = "/tmp/delta/test/checkpoint"


  def createIntialBronzeTable(): Unit = {

    spark.sql("drop table if exists test_bronze.employee")
    spark.sql(
      """
        |create table test_bronze.employee (
        |empid int, ename string, sal double, deptno int,comm int)
        |using delta
        |""".stripMargin)
    val empdf = spark.read.option("header", value = false).schema("empid int, ename string, sal double, deptno int,comm int").csv("src/test/resources/emp.csv")
    empdf.write.format("delta").mode("append").saveAsTable("test_bronze.employee")
    empdf.write.format("delta").mode("overwrite").save(inputPathAppend)

  }

  def createIntialSilverTable(): Unit = {
    //create intial records in silver
    spark.sql("drop table if exists test_silver.employee")
    spark.sql(
      """
        |create table test_silver.employee (
        |empid int, ename string, sal double, deptno int,comm int)
        |using delta
        |""".stripMargin)
    val empdf = spark.read.option("header", value = false).schema("empid int, ename string, sal double, deptno int,comm int").csv("src/test/resources/emp.csv")
    empdf.write.format("delta").mode("append").saveAsTable("test_silver.employee")
  }

  override protected  def beforeAll(): Unit = {
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

  override protected  def afterAll(): Unit = {
    deltaDir.deleteRecursively()
    spark.stop()
  }

  test("check writescd1 function standalone") {
    val df = spark.read.table("test_bronze.employee")
    df.show(false)

    //assert before merge
    assert(df.count() == 5)
    assert(df.filter("empid= '5467'").select("sal").collect()(0)(0) == 247.0)
    assert(df.filter("empid= '5468'").count() == 1)
    assert(df.filter("ename= 'sriram'").count() == 0)

    val empUpdateDF = spark.read.option("header", value = false).schema("empid int, ename string, sal double, deptno int,comm int").csv("src/test/resources/emp_update.csv")


        writeSCD1(spark,
          empUpdateDF
        ,"test_bronze.employee"
        , Seq("empid")
        , Some(Seq(MergeBuilderLogicSCD1("match",Some(DELETE),Some("target.comm is null"),None)
                  ,MergeBuilderLogicSCD1("match",None,None,Some(Map("target.sal" -> "updates.sal")))
                  ,MergeBuilderLogicSCD1("notmatch",None,None,None)))
        , None)

    df.show(false)

    //assert after merge
    assert(df.count() == 5)
    assert(df.filter("empid= '5467'").select("sal").collect()(0)(0) == 500.0)
    assert(df.filter("empid= '5468'").count() == 0)
    assert(df.filter("ename= 'sriram'").count() == 1)
  }

  test("check writescd1 with processOutputStreamBatch"){
    createIntialSilverTable()

    //load updated records in to a empUpdateDF
    val empUpdateDF = spark.read.option("header", value = false).schema("empid int, ename string, sal double, deptno int,comm int").csv("src/test/resources/emp_update.csv")
    empUpdateDF.write.format("delta").mode("overwrite").save(inputPathForEachBatch)
    val inputStream = spark.readStream.format("delta").load(inputPathForEachBatch)

    //create output stream config
    val outputStreamConf : WriteStreamConfig = WriteStreamConfig(
      format = Some("delta"),
      triggerMode = Some("ProcessingTime"),
      triggerDuration = Some("10 seconds"))


    val df = spark.read.table("test_silver.employee")
    //assert before merge
    assert(df.count() == 5)
    assert(df.filter("empid= '5467'").select("sal").collect()(0)(0) == 247.0)
    assert(df.filter("empid= '5468'").count() == 1)
    assert(df.filter("ename= 'sriram'").count() == 0)

    //calling the processOutputStreamBatch to apply foreachbatch on mini batch
    ProcessOutputStream.processOutputStreamBatch(
      Some("test"),
      inputStream,
      "test_silver.employee",
      (batchDf: Dataset[Row], batchId: Long) =>
                                      writeSCD1(spark,batchDf
                                                ,"test_silver.employee"
                                                , Seq("empid")
                                                , Some(Seq(MergeBuilderLogicSCD1("match",Some(DELETE),Some("target.comm is null"),None)
                                                          ,MergeBuilderLogicSCD1("match",None,None,Some(Map("target.sal" -> "updates.sal")))
                                                          ,MergeBuilderLogicSCD1("notmatch",None,None,None)))
                                                , None),
      outputStreamConf
    )

    //assert after merge
    assert(df.count() == 5)
    assert(df.filter("empid= '5467'").select("sal").collect()(0)(0) == 500.0)
    assert(df.filter("empid= '5468'").count() == 0)
    assert(df.filter("ename= 'sriram'").count() == 1)
  }

  test("check processOutputStream with append mode"){
    val inputStream = spark.readStream.format("delta").load(inputPathAppend)

    //create output stream config
    val outputStreamConf : WriteStreamConfig = WriteStreamConfig(
      format = Some("delta"),
      outputMode = Some("append"),
      checkpointLocation = Some(s"${checkPointLoc}/employee_append"),
      triggerMode = Some("ProcessingTime"),
      triggerDuration = Some("10 seconds"))

    //calling the processOutputStreamBatch to apply foreachbatch on mini batch
    ProcessOutputStream.processOutputStream(
      Some("test"),
      inputStream,
      "test_silver.employee_append",
      outputStreamConf
    )

    val df = spark.read.table("test_silver.employee_append")
    assert(df.count() > 0)

  }


 /* test("check writescd1 with processOutputStreamBatch 2"){
    createIntialSilverTable()

    //load updated records in to a empUpdateDF
    val empUpdateDF = spark.read.option("header", value = false).schema("empid int, ename string, sal double, deptno int,comm int").csv("src/test/resources/emp_update.csv")
    empUpdateDF.write.format("delta").mode("overwrite").save(inputPathForEachBatch)
    val inputStream = spark.readStream.format("delta").load(inputPathForEachBatch)

    //create output stream config
    val outputStreamConf : WriteStreamConfig = WriteStreamConfig(
      format = Some("delta"),
      triggerMode = Some("ProcessingTime"),
      triggerDuration = Some("10 seconds"))


    val df = spark.read.table("test_silver.employee")
    df.show(false)
    //assert before merge
    assert(df.count() == 5)
    assert(df.filter("empid= '5467'").select("sal").collect()(0)(0) == 247.0)
    assert(df.filter("empid= '5468'").select("sal").collect()(0)(0) == 5000.0)
    assert(df.filter("ename= 'sriram'").count() == 0)

    //calling the processOutputStreamBatch to apply foreachbatch on mini batch
    ProcessOutputStream.processOutputStreamBatch(
      Some("test"),
      inputStream,
      "test_silver.employee",
      (batchDf: Dataset[Row], batchId: Long) =>
        writeSCD1(batchDf
          ,"test_silver.employee"
          , Seq("empid = empid")
          , None
          , None),
      outputStreamConf
    )

    //assert after merge
    assert(df.count() == 6)
    assert(df.filter("empid= '5467'").select("sal").collect()(0)(0) == 500.0)
    assert(df.filter("empid= '5468'").select("sal").collect()(0)(0) == 5500.0)
    assert(df.filter("ename= 'sriram'").count() == 1)
    df.show(false)
  }*/



}
