package com.databricks.genericPipelineOrchestrator.utility

import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant.{ ERROR_TABLE_NAME, FACT_TABLE_NAME, ORCHESTRATION_DB, STATUS_TABLE_NAME, TABLE_TOPIC_DETAILS_TABLE_NAME}
import com.databricks.genericPipelineOrchestrator.commons.{ErrorTableColumns, FactColumns, StatusColumns}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object OrchestrationLocalCreateTable {val logger = Logger.getLogger(getClass)
  val DB_LOCATION = "/tmp/delta/test"
  val METADATA_DB = "pipeline_metadata_db"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    createDatabase(spark)
    createStatusTable(spark)
   // createFactTable(spark)
   // createErrorLogTable(spark)


  }
  def initFramework(spark:SparkSession)={
    createDatabase(spark)
    createStatusTable(spark)
    createFactTable(spark)
    createErrorLogTable(spark)
  }

  def createDatabase(spark:SparkSession)={
    //  val deltaDir = Directory(new File("/tmp/delta/test"))
    // deltaDir.deleteRecursively()
    spark.sql(s"""CREATE DATABASE ${ORCHESTRATION_DB} LOCATION '/tmp/delta'""")
    logger.info(s"database created ${ORCHESTRATION_DB}")
  }

  def createStatusTable(spark:SparkSession)={
    val createStatement =
      s"""CREATE TABLE ${ORCHESTRATION_DB}.${STATUS_TABLE_NAME}
         |(${StatusColumns.topic.toString} String,
         |${StatusColumns.partition.toString} Integer,
         |${StatusColumns.startOffset.toString} Long,
         |${StatusColumns.endOffset.toString} Long,
         |${StatusColumns.pipelineName.toString} String,
         |${StatusColumns.instanceName.toString} String,
         |${StatusColumns.status.toString} String,
         |${StatusColumns.pipelineId.toString} String,
         |${StatusColumns.runId.toString} String,
         |${StatusColumns.errorMsg.toString} String,
         |${StatusColumns.stackTrace.toString} String,
         |${StatusColumns.lastUpdate.toString} timestamp,
         |${StatusColumns.lastUpdateDate.toString} Date) USING DELTA LOCATION '${DB_LOCATION}/${STATUS_TABLE_NAME}' """.stripMargin
    spark.sql(createStatement);
    logger.info(s"Table created ${ORCHESTRATION_DB}.${STATUS_TABLE_NAME} ")
  }

  def createFactTable(spark:SparkSession)={
    val createStatement =
      s"""CREATE TABLE ${ORCHESTRATION_DB}.${FACT_TABLE_NAME}
         |(${FactColumns.runId.toString} String,
         |${FactColumns.pipelineId.toString} String,
         |${FactColumns.pipelineName.toString} STRING,
         |${FactColumns.instanceName.toString} STRING,
         |${FactColumns.topic.toString} String,
         |${FactColumns.inputCount.toString} Long,
         |${FactColumns.outputCount.toString} Long,
         |${FactColumns.lastUpdate.toString} timestamp)  USING DELTA LOCATION '${DB_LOCATION}/${FACT_TABLE_NAME}' """.stripMargin
    spark.sql(createStatement);
    logger.info(s"Table created ${ORCHESTRATION_DB}.${FACT_TABLE_NAME} ")
  }

  def createErrorLogTable(spark:SparkSession)={
    val createStatement =
      s"""CREATE TABLE ${ORCHESTRATION_DB}.${ERROR_TABLE_NAME}
         |(${ErrorTableColumns.errorData.toString} String,
         |${ErrorTableColumns.insertTime.toString} timestamp)  USING DELTA LOCATION '${DB_LOCATION}/${ERROR_TABLE_NAME}' """.stripMargin
    spark.sql(createStatement);
    logger.info(s"Table created ${ORCHESTRATION_DB}.${ERROR_TABLE_NAME} ")
  }


  def createLocalTableDetails(spark: SparkSession)={
    spark.sql(s"CREATE DATABASE $METADATA_DB LOCATION '/tmp/delta/test/$METADATA_DB'")

    spark.sql(
      s"""
         |CREATE TABLE $METADATA_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME (
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
         |  extra_join_cond string,
         |  join_key string)
         |USING delta
         |""".stripMargin)


    val readCSV = spark.read.option("header",value = true)
      .option("delimiter","|")
      .option("quote", "\"")
      .option("escape", "\"")
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
          |  extra_join_cond string,
          |   join_key string""".stripMargin).csv("src/test/resources/table_details_op.csv")
    readCSV.show(false)
    readCSV.write.format("delta").mode("append").saveAsTable(s"$METADATA_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME")

  }

}
