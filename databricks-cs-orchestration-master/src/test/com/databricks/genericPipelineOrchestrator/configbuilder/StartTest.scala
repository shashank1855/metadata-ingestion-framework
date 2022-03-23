package com.databricks.genericPipelineOrchestrator.configbuilder

import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant.{ORCHESTRATION_DB, SHARD_DETAILS_TABLE_NAME, TABLE_TOPIC_DETAILS_TABLE_NAME}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.reflect.io.Directory

class StartTest extends AnyFunSuite with BeforeAndAfterAll {

  @transient var spark: SparkSession = null
  @transient var deltaDir: Directory = null

  override protected  def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    deltaDir = Directory(new File("/tmp/delta/test"))
    deltaDir.deleteRecursively()

    spark.sql(s"CREATE DATABASE $ORCHESTRATION_DB LOCATION '/tmp/delta/test/$ORCHESTRATION_DB'")

    spark.sql(
      s"""
         |CREATE TABLE $ORCHESTRATION_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME (
         |  `pipeline_def_id` STRING,
         |  `product_name` STRING,
         |  `table_name` STRING,
         |  `table_type` STRING,
         |  `primary_key` STRING,
         |  `partition_id_col` STRING,
         |  `updated_at_col` STRING,
         |  `src_table_schema` STRING,
         |  `src_table_cols` STRING,
         |  `import_table_schema` STRING,
         |  `import_table_cols` STRING,
         |  `omitted_cols` STRING,
         |  `table_size` STRING,
         |  `remarks` STRING,
         |  `join_key` STRING,
         |  `scd_type` STRING,
         |  `op_config` STRING,
         |  `merge_cond` STRING,
         |  `extra_join_cond` STRING,
         |  `partition_size` INT,
         |  `reader_type` STRING,
         |  `reader_options` STRING)
         |USING delta
         |""".stripMargin)


    val readCSV = spark.read.option("header",value = true)
      .option("delimiter",",")
      .option("quote", "\"")
      .option("escape", "\"")
      .schema(
      """
        |`pipeline_def_id` STRING,
        |  `product_name` STRING,
        |  `table_name` STRING,
        |  `table_type` STRING,
        |  `primary_key` STRING,
        |  `partition_id_col` STRING,
        |  `updated_at_col` STRING,
        |  `src_table_schema` STRING,
        |  `src_table_cols` STRING,
        |  `import_table_schema` STRING,
        |  `import_table_cols` STRING,
        |  `omitted_cols` STRING,
        |  `table_size` STRING,
        |  `remarks` STRING,
        |  `join_key` STRING,
        |  `scd_type` STRING,
        |  `op_config` STRING,
        |  `merge_cond` STRING,
        |  `extra_join_cond` STRING,
        |  `partition_size` INT,
        |  `reader_type` STRING,
        |  `reader_options` STRING """.stripMargin).csv("src/test/resources/table_details_op.csv")
    readCSV.write.format("delta").mode("append").saveAsTable(s"$ORCHESTRATION_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME")

    spark.sql(
      s"""
         |CREATE TABLE $ORCHESTRATION_DB.pipeline_batch_map (
         |  pipeline_def_id string,
         |  batch_id string)
         |USING delta
         |""".stripMargin)
    spark.sql(
      s"""
         |select
         |'test_users' as pipeline_def_id,
         |'test01' as batch_id union
         |select
         |'test_accounts' as pipeline_def_id,
         |'test01' as batch_id
         |""".stripMargin).write.format("delta").mode("append").saveAsTable(s"$ORCHESTRATION_DB.pipeline_batch_map")


    spark.sql(
      s"""
         |CREATE TABLE $ORCHESTRATION_DB.$SHARD_DETAILS_TABLE_NAME (
         |  `shard_def_id` STRING,
         |  `product_name` STRING,
         |  `region_name` STRING,
         |  `shard_name` STRING,
         |  `shard_type` STRING,
         |  `endpoint` STRING,
         |  `database_name` STRING,
         |  `secret_scope` STRING,
         |  `secret_user` STRING,
         |  `secret_password` STRING)
         |USING delta
         |""".stripMargin)

    val readSHARDCSV = spark.read.option("header",value = true)
      .schema(
        """
          |`shard_def_id` STRING,
          |  `product_name` STRING,
          |  `region_name` STRING,
          |  `shard_name` STRING,
          |  `shard_type` STRING,
          |  `endpoint` STRING,
          |  `database_name` STRING,
          |  `secret_scope` STRING,
          |  `secret_user` STRING,
          |  `secret_password` STRING """.stripMargin).csv("src/test/resources/shard_details.csv")
    readSHARDCSV.write.format("delta").mode("append").saveAsTable(s"$ORCHESTRATION_DB.$SHARD_DETAILS_TABLE_NAME")



    object StatusColumns extends Enumeration {
      val topic, partition, startOffset, endOffset, pipelineName, instanceName, status,pipelineId,runId,errorMsg,stackTrace,lastUpdate,batchId,pipelineDefId,lastUpdateDate = Value
    }

    val dbName = "orchestration_db"

    val createStatement =
      s"""CREATE TABLE if not exists ${dbName}.pipeline_status
         |(${StatusColumns.topic.toString} String,
         |${StatusColumns.partition.toString} Integer,
         |${StatusColumns.startOffset.toString} Long,
         |${StatusColumns.endOffset.toString} Long,
         |${StatusColumns.pipelineName.toString} String,
         |${StatusColumns.pipelineDefId.toString} String,
         |${StatusColumns.batchId.toString} String,
         |${StatusColumns.instanceName.toString} String,
         |${StatusColumns.status.toString} String,
         |${StatusColumns.pipelineId.toString} String,
         |${StatusColumns.runId.toString} String,
         |${StatusColumns.errorMsg.toString} String,
         |${StatusColumns.stackTrace.toString} String,
         |${StatusColumns.lastUpdate.toString} timestamp,
          ${StatusColumns.lastUpdateDate.toString} date) USING DELTA
         |""".stripMargin
    spark.sql(createStatement)
  }

  override protected  def afterAll(): Unit = {
    deltaDir.deleteRecursively()
    spark.stop()
  }

  test("1"){
    //Start.execute(spark,Array("test01,test02"))

  }


  test("2"){
    spark.read.table(s"$ORCHESTRATION_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME").show(false)
    spark.read.table(s"$ORCHESTRATION_DB.$SHARD_DETAILS_TABLE_NAME").show(false)
    JDBCLoadStart.execute(spark,Array("test_shard_30"))

  }
}