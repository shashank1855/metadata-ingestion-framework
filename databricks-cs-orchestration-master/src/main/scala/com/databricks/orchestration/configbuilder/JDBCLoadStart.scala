package com.databricks.genericPipelineOrchestrator.configbuilder

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.genericPipelineOrchestrator.Pipeline.{Pipeline, PipelineBuilder}
import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant._
import com.databricks.genericPipelineOrchestrator.commons.PipeLineType
import com.databricks.genericPipelineOrchestrator.processor.ProcessorBuilder
import com.databricks.genericPipelineOrchestrator.reader.ReaderBuilder
import com.databricks.genericPipelineOrchestrator.writter.config.WriteStreamConfig
import com.databricks.genericPipelineOrchestrator.writter.{MergeBuilderLogicSCD1, MergeBuilderLogicSCD2, WriterBuilder, mergeSCD1Options, mergeSCD2Options, mergeSCD4Options}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Row, SparkSession}
import org.json.JSONArray
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse

import java.util
import java.util.concurrent.Executors
import scala.collection.mutable

object JDBCLoadStart {

  val logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")
    if (args.isEmpty) {
      val message = s"Please pass atleast one argument"
      throw new Exception(message)
    }
    val shardDefId = args(0).split(",")
    if (shardDefId.size > 1) {
      val message = s"""Please pass only one shard_def_id"""
      throw new Exception(message)
    }
    execute(spark, args)

  }


  def execute(spark: SparkSession, args: Array[String]): Unit = {

    val shardDetailsTableDf = spark.sql(
      s"""
         |select * from $ORCHESTRATION_DB.$SHARD_DETAILS_TABLE_NAME where shard_def_id in (${args(0).split(",").mkString("'", "','", "'")})
         |""".stripMargin).cache()
    shardDetailsTableDf.createGlobalTempView("shard_details")

    shardDetailsTableDf.show(false)


    val productName = spark.sql("select * from global_temp.shard_details").select("product_name").distinct().collect().map(r => r.getAs("product_name").toString)
    val tableDetailsDf = spark.sql(
      s"""
         |select distinct table_details.*,mapping.batch_id from
         |$ORCHESTRATION_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME table_details
         |inner join $ORCHESTRATION_DB.$PIPELINE_BATCH_MAP mapping on (table_details.pipeline_def_id = mapping.pipeline_def_id)
         |where product_name in (${productName.mkString("'", "','", "'")})
         |and table_name='accounts'
         |""".stripMargin)

    tableDetailsDf.show(false)

    val pipelineList = new util.ArrayList[Pipeline]()
    val runId = java.util.UUID.randomUUID.toString

    tableDetailsDf.collect().foreach(r => {
      val (topicName, productName, mergeSCD1Obj, mergeSCD2Obj, mergeSCD4Obj, scdType, batchId, partitionSize, pipelineDefId) = extractFields(r)


      var pipelineBuilder = PipelineBuilder.start().addSparkSession(spark)
        .setPipelineName(topicName)
        .setPipelineType(PipeLineType.JDBC)
        .setPipelineDefId(pipelineDefId)
        .setProductName(productName)
        .setBatchId(batchId)
        .setTableName(topicName)
        .setRunId(runId)
        .addTask(topicName + JDBC_READER, ReaderBuilder.start().getSqlJdbcReader())

      pipelineBuilder = scdType match {
        case "scd1" => pipelineBuilder
          .setMergeSCD1Options(mergeSCD1Obj)
          .addAfter(topicName + JDBC_READER, topicName + JDBC_PROCESSOR, ProcessorBuilder.start().getProcessorDeltaStreamSCD1)
          .addAfter(topicName + JDBC_PROCESSOR, topicName + JDBC_WRITER, WriterBuilder.start().getDeltaWriterSCD1)
        case "scd2" => pipelineBuilder
          .setMergeSCD2Options(mergeSCD2Obj)
          .addAfter(topicName + JDBC_READER, topicName + JDBC_PROCESSOR, ProcessorBuilder.start().getProcessorDeltaStreamSCD2)
          .addAfter(topicName + JDBC_PROCESSOR, topicName + JDBC_WRITER, WriterBuilder.start().getDeltaWriterSCD2)
        case "scd4" => pipelineBuilder
          .setMergeSCD4Options(mergeSCD4Obj)
          .addAfter(topicName + JDBC_READER, topicName + JDBC_PROCESSOR, ProcessorBuilder.start().getProcessorDeltaStreamSCD4)
          .addAfter(topicName + JDBC_PROCESSOR, topicName + JDBC_WRITER, WriterBuilder.start().getDeltaWriterSCD4)
      }

      logger.info(s"TopicName is $topicName")
      scdType match {
        case "scd1" => logger.info(mergeSCD1Obj)
        case "scd2" => logger.info(mergeSCD2Obj)
        case "scd4" => logger.info(mergeSCD4Obj)
      }

      pipelineList.add(pipelineBuilder.build())

    })

    println(pipelineList.size())
    logger.info("INFO: Inside Start after PipelineBuilder")
    if (pipelineList.size() > 0) {
      val es = Executors.newFixedThreadPool(pipelineList.size())
      es.invokeAll(pipelineList)
    } else {
      logger.info("Nothing to execute")
    }


  }

  /**
   * This function helps in extracting data from metadata config
   *
   * @param r row
   * @return
   */

  def extractFields(r: Row): (String, String, mergeSCD1Options, mergeSCD2Options, mergeSCD4Options, String, String, Int, String) = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    val topicName = r.getAs("table_name").toString
    val productName = r.getAs("product_name").toString
    val partitionSize = r.getAs[Int]("partition_size")
    val pipelineDefId = r.getAs("pipeline_def_id").toString
    val jsonString = if (r.getAs("op_config") == null) "" else {
      r.getAs("op_config").toString
    }
    val outputStreamConf = parse(jsonString).extract[WriteStreamConfig]
    val SCDType = r.getAs("scd_type").toString
    val batchId = r.getAs("batch_id").toString

    val seq = scala.collection.mutable.ArrayBuffer[MergeBuilderLogicSCD1]()
    var logicSCD2: MergeBuilderLogicSCD2 = null
    if (SCDType == "scd1") {
      val mergeJsonArray = if (r.getAs("merge_cond") == null) "[]" else {
        r.getAs("merge_cond").toString
      }
      val ja = new JSONArray(mergeJsonArray)
      for (i <- 0 until ja.length()) {
        val jo = ja.getJSONObject(i).toString()
        seq += parse(jo).extract[MergeBuilderLogicSCD1]
      }
    } else if (SCDType == "scd2") {
      try {
        val mergeJsonObject = if (r.getAs("merge_cond") == null) "{}" else {
          r.getAs("merge_cond").toString
        }
        logicSCD2 = parse(mergeJsonObject).extract[MergeBuilderLogicSCD2]
      }
      catch {
        case e: Exception =>
          logger.error(s"Execption in $topicName while creating MergeBuilderLogicSCD2")
          logger.error(e.getMessage, e)
          throw new Exception(e)
      }
    }
    val logicSCD1 = if (seq.isEmpty) None else Some(seq)
    var joinKeys: Seq[String] = Seq[String]()
    if (r.getAs("join_key") == null) {
      val primaryKey = r.getAs("primary_key").toString
      joinKeys = Seq(primaryKey.split(",")).flatten.distinct
    } else {
      joinKeys = Seq(r.getAs("join_key").toString.split(",")).flatten
    }

    var partitionKeys: Option[Seq[String]] = None
    if (r.getAs("partition_id_col") != null) {
      val partitionKey = r.getAs("partition_id_col").toString
      partitionKeys = Some(Seq(partitionKey.split(",")).flatten.distinct)
    }

    var dedupKeys: Option[Seq[String]] = None
    if (r.getAs("updated_at_col") != null) {
      val dedupKey = r.getAs("updated_at_col").toString
      dedupKeys = Some(Seq(dedupKey.split(",")).flatten.distinct)
    }

    val extraJoinCond = if (r.getAs("extra_join_cond") == null) None else {
      Some(r.getAs("extra_join_cond").toString)
    }
    val mergeSCD1Obj = mergeSCD1Options(outputStreamConf, logicSCD1, joinKeys, partitionKeys, dedupKeys, extraJoinCond)
    val mergeSCD2Obj = mergeSCD2Options(outputStreamConf, logicSCD2, joinKeys, partitionKeys, dedupKeys, extraJoinCond)
    val mergeSCD4Obj = mergeSCD4Options(outputStreamConf, joinKeys, partitionKeys, dedupKeys, extraJoinCond)
    (topicName, productName, mergeSCD1Obj, mergeSCD2Obj, mergeSCD4Obj, SCDType, batchId, partitionSize, pipelineDefId)

  }

}
