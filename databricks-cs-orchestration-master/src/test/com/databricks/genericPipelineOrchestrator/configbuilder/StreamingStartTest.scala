/*
package com.databricks.genericPipelineOrchestrator.configbuilder

import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant.{DB_TEST_DB, METADATA_DB, STATUS_TABLE_NAME, TABLE_TOPIC_DETAILS_TABLE_NAME}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import scala.reflect.io.Directory

class StreamingStartTest extends AnyFunSuite with BeforeAndAfterAll {

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

    spark.sql(s"CREATE DATABASE $DB_TEST_DB LOCATION '/tmp/delta/test'")

    spark.sql(
      s"""
         |CREATE TABLE $DB_TEST_DB.$STATUS_TABLE_NAME (
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
         |  scd_type String,
         |  op_config String,
         |  merge_cond String,
         |  extra_join_cond string)
         |USING delta
         |""".stripMargin)


    val readCSV = spark.read.option("header",value = true)
      .option("delimiter","|")
      .schema(
        """
          |`product_name` STRING,
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
          |   scd_type String,
          |  op_config String,
          |  merge_cond String,
          |  extra_join_cond string """.stripMargin).csv("src/test/resources/table_details_op.csv")
    readCSV.write.format("delta").mode("append").saveAsTable(s"$METADATA_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME")


  }

  override protected  def afterAll(): Unit = {
    deltaDir.deleteRecursively()
    spark.stop()
  }

  test("1"){
    Start.fun(spark,null)

  }

}*/
