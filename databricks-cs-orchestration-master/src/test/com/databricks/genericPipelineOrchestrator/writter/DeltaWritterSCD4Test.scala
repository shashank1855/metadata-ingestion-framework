package com.databricks.genericPipelineOrchestrator.writter

import com.databricks.genericPipelineOrchestrator.Pipeline.{Pipeline, PipelineBuilder}
import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant._
import com.databricks.genericPipelineOrchestrator.processor.ProcessorBuilder
import com.databricks.genericPipelineOrchestrator.reader.ReaderBuilder
import com.databricks.genericPipelineOrchestrator.utility.Utility.{writeSCD4, writeStreamSCD4}
import com.databricks.genericPipelineOrchestrator.writter.config.WriteStreamConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.util
import java.util.concurrent.Executors
import scala.reflect.io.Directory

class DeltaWritterSCD4Test extends AnyFunSuite with BeforeAndAfterAll {
  @transient var spark: SparkSession = null
  @transient var deltaDir: Directory = null
  @transient val employee_details = "/tmp/delta/test/employee_details"
  @transient val employee_current_details_table = s"${ORCHESTRATION_DB}.employee"
  @transient val employee_history_details_table = s"${ORCHESTRATION_DB}.employee${HISTORY_TABLE_TAG}"

  def createIntialCurrentTable(): Unit = {

    spark.sql(s"drop table if exists $employee_current_details_table")
    spark.sql(
      s"""
         |create table $employee_current_details_table (
         |empid int, ename string, sal double, deptno int,comm int, place string, effDate timestamp,deleted_flag boolean)
         |using delta
         |""".stripMargin)
    val empdf = spark.read.option("header", value = false).schema("empid int, ename string, sal double, deptno int,comm int,place string, effDate timestamp,deleted_flag boolean").csv("src/test/resources/emp_scd4.csv")
    empdf.write.format("delta").mode("append").saveAsTable(employee_current_details_table)


    spark.sql(
      """
        |CREATE TABLE if not exists `orchestration_db`.`PIPELINE_STATUS` (
        |  `topic` STRING,
        |  `partition` INT,
        |  `startOffset` BIGINT,
        |  `endOffset` BIGINT,
        |  `pipelineName` STRING,
        |  `pipelineDefId` STRING,
        |  `batchId` STRING,
        |  `instanceName` STRING,
        |  `status` STRING,
        |  `pipelineId` STRING,
        |  `runId` STRING,
        |  `errorMsg` STRING,
        |  `stackTrace` STRING,
        |  `lastUpdate` TIMESTAMP,
        |  `lastUpdateDate` DATE)
        |USING delta
        |--PARTITIONED BY (lastUpdateDate, pipelineDefId)
        |TBLPROPERTIES (
        |  'delta.autoOptimize.optimizeWrite' = 'true',
        |  'delta.autoOptimize.autoCompact' = 'true')
        |""".stripMargin)
    spark.sql(
      """
        |select
        |null as topic
        |,null as partition
        |,null as startOffset
        |,null as endOffset
        |,null as pipelineName
        |,null as instanceName
        |,null as status
        |,null as pipelineId
        |,null as runId
        |,null as errorMsg
        |,null as stackTrace
        |,null as lastUpdate
        |,null as pipelineDefId
        |,null as batchId
        |,null as lastUpdateDate
        |""".stripMargin).write.format("delta").mode("append").saveAsTable("orchestration_db.PIPELINE_STATUS")

    spark.sql(
      s"""
         |CREATE TABLE if not exists `orchestration_db`.`pipeline_fact` (
         |  `runId` STRING,
         |  `pipelineId` STRING,
         |  `pipelineName` STRING,
         |  `pipelineDefId` STRING,
         |  `instanceName` STRING,
         |  `topic` STRING,
         |  `inputCount` BIGINT,
         |  `outputCount` BIGINT,
         |  `lastUpdate` TIMESTAMP,
         |  `lastUpdateDate` DATE,
         |  `operationMetrics` MAP<STRING, STRING>)
         |USING delta
         |PARTITIONED BY (lastUpdateDate, pipelineDefId)
         |""".stripMargin)
    spark.sql(
      s"""
         |select
         |null as runId,
         |null as pipelineId,
         |null as pipelineName,
         |null as topic,
         |null as inputCount,
         |null as outputCount,
         |null as lastUpdate,
         |null as lastUpdateDate,
         |null as operationMetrics,
         |null as pipelineDefId,
         |null as instanceName
         |""".stripMargin).write.format("delta").partitionBy("lastUpdateDate","pipelineDefId").mode("append").saveAsTable("orchestration_db.PIPELINE_FACT")
  }

  def createIntialHisttable(): Unit = {

    spark.sql(s"drop table if exists $employee_history_details_table")
    spark.sql(
      s"""
         |create table $employee_history_details_table (
         |empid int, ename string, sal double, deptno int,comm int, place string, effDate timestamp,history_created_at timestamp, deleted_flag boolean)
         |using delta
         |PARTITIONED by (empid)
         |""".stripMargin)
    val empdf = spark.read.option("header", value = false).schema("empid int, ename string, sal double, deptno int,comm int,place string, effDate timestamp,history_created_at timestamp,deleted_flag boolean").csv("src/test/resources/emp_scd4_hist.csv")
    empdf.write.format("delta").mode("append").partitionBy("empid").saveAsTable(employee_history_details_table)

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
    spark.sql(s"create database $ORCHESTRATION_DB LOCATION '/tmp/delta/test/$ORCHESTRATION_DB'")
    createIntialCurrentTable()
    createIntialHisttable()
  }

  override protected def afterAll(): Unit = {
    spark.read.table("orchestration_db.PIPELINE_STATUS").show(1000,false)
    spark.read.table("`orchestration_db`.`PIPELINE_FACT`").show(1000,false)
    deltaDir.deleteRecursively()
    spark.stop()
  }

  test("test writeSCd4 with streaming") {

    val runId = java.util.UUID.randomUUID.toString
    val topicName = "employee"
    val productName = "test"
    val outputStreamConf = WriteStreamConfig(format = Some("delta"), triggerMode = Some("Once"))
    val mergeSCD4Obj = mergeSCD4Options(outputStreamConf, Seq("empid"),None,None, None)


    val empUpdateDF = spark.readStream.option("header", value = false).schema("empid int, ename string, sal double, deptno int,comm int,place string, effDate timestamp").csv("src/test/resources/scd4/").withColumn("updated_at", col("effDate"))


    val deltaWriterObj = WriterBuilder.start().getStreamDeltaWriterSCD4
    deltaWriterObj.inputDataframe = scala.collection.mutable.Map(RAWDF -> empUpdateDF, PROCESSEDDF -> empUpdateDF)

    val df = spark.read.table(employee_current_details_table)
    df.orderBy("empid", "place").show(false)
    val df_hist = spark.read.table(employee_history_details_table)
    df_hist.orderBy("empid", "place").show(false)
    println("Before change")

    val pipelineList = new util.ArrayList[Pipeline]()
    pipelineList.add(PipelineBuilder.start()
      .setPipelineName(topicName)
      .setProductName(productName)
      .setTopicName(topicName)
      .setRunId(runId)
      .addSparkSession(spark)
      .setMergeSCD4Options(mergeSCD4Obj)
      .addTask(topicName + WRITER, deltaWriterObj)
      .build())

    val es = Executors.newFixedThreadPool(pipelineList.size())
    es.invokeAll(pipelineList)

    df.orderBy("empid", "place").show(false)
    df_hist.orderBy("empid", "place").show(false)
    println("After change")

  }

  test("test SCD4 standalone") {
    createIntialCurrentTable()
    createIntialHisttable()
    val empUpdateDF = spark.read.option("header", value = false).schema("empid int, ename string, sal double, deptno int,comm int,place string, effDate timestamp,row_active boolean,deleted_flag boolean").csv("src/test/resources/scd4/emp_scd4_update.csv").withColumn("updated_at", col("effDate"))
    val df = spark.read.table(employee_current_details_table)
    df.orderBy("empid", "place").show(false)
    val df_hist = spark.read.table(employee_history_details_table)
    df_hist.orderBy("empid", "place").show(false)
    println("Before change")

    empUpdateDF.orderBy("empid", "place").show(false)
    println("empUpdateDF")
    writeSCD4(spark,empUpdateDF,employee_current_details_table,Seq("empid"),None,Some(Seq("empid")), Some(Seq("effDate")))

    df.orderBy("empid", "place").show(false)
    df_hist.orderBy("empid", "place").show(false)
    println("After change")



  }

}
