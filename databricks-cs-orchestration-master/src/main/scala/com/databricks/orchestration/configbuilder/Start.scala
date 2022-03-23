package com.databricks.genericPipelineOrchestrator.configbuilder

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
import org.json4s.{DefaultFormats, _}
import org.json4s.native.JsonMethods._

import java.util
import java.util.concurrent.Executors

object Start {
  val logger = Logger.getLogger(getClass)

  /**
   * The Main function to start the Orchestration please provide at lest one argument
   *  1.For Normal Run give comma separated BatchIds as input
   *  2.For Rerun of a particular pipeline which got error in a batch give comma separated batch id as input with RERUN_BATCH as keyword EX: RERUN_BATCH batch01,batch02
   *  3.To run a particular pipeline give comma separated pipeline name with PIPELINE as keyword EX: PIPELINE social_twitter_test,social_streams_test
   * @param args
   */
  def main(args: Array[String]): Unit = {
  val spark: SparkSession = SparkSession
    .builder()
    .getOrCreate()
  //spark.sparkContext.setLogLevel("ERROR")
    if(args.isEmpty) {
      val message = s"Please pass atleast one argument" +
        s" 1.For Normal Run give space separated BatchIds " +
        s" 2.For Rerun of a particular pipeline which got error in a batch give input as : RERUN_BATCH "+
        s" 3.For Rerun of a particular pipeline  : PIPELINE social_twitter_test,social_streams_test"
      throw new Exception(message)
    }
    execute(spark,args)

}

  /**
   * ENtry point for the framework
   * @param spark spark
   * @param args argumnets
   */
  def execute(spark: SparkSession, args: Array[String]): Unit = {

    logger.info("INFO: Inside Start Before PipelineBuilder")
    logger.info("Input args :"+args.mkString)
    var arg=args(0).split(" ")
    arg(0) match {

      case "RERUN_BATCH"=>{
        logger.info("RERUN_BATCH :"+arg(1))
        rerunErrorBatch(spark,arg(1).replaceAll(",","','"))
      };
      case "PIPELINE"=>{
        logger.info("PIPELINE :"+arg(1))
        runPipeline(spark,arg(1).replaceAll(",","','"))
      };
      case _=>{
        logger.info("Default Run BATCH ID:"+arg(0))
        defaultRun(spark,arg(0).replaceAll(",","','"))
      };
    }
    val df = spark.sql("select * from metaTable".stripMargin)
    df.show(false)
    val pipelineList = new util.ArrayList[Pipeline]()
    val runId = java.util.UUID.randomUUID.toString
    df.collect().foreach(r => {
      val (topicName,productName,mergeSCD1Obj,mergeSCD2Obj,mergeSCD4Obj,scdType,batchId,partitionSize,pipelineDefId) = extractFields(r)
      var pipelineBuilder = PipelineBuilder.start().addSparkSession(spark)
        .setPipelineName(topicName)
        .setPipelineType(PipeLineType.CDC)
        .setPipelineDefId(pipelineDefId)
        .setProductName(productName)
        .setBatchId(batchId)
        .setPartitionSize(partitionSize)
        .setTopicName(topicName)
        .setTableName(topicName)
        .setRunId(runId)
        .addTask(topicName + CDC_READER, ReaderBuilder.start().getKafkaReader())


      pipelineBuilder = scdType match {
        case "scd1" => pipelineBuilder
          .setMergeSCD1Options(mergeSCD1Obj)
          .addAfter(topicName + CDC_READER, topicName + CDC_PROCESSOR, ProcessorBuilder.start().getProcessorStreamSCD1)
          .addAfter(topicName + CDC_PROCESSOR, topicName + CDC_WRITER, WriterBuilder.start().getStreamDeltaWriterSCD1)
        case "scd2" => pipelineBuilder
          .setMergeSCD2Options(mergeSCD2Obj)
          .addAfter(topicName + CDC_READER, topicName + CDC_PROCESSOR, ProcessorBuilder.start().getProcessorStreamSCD2)
          .addAfter(topicName + CDC_PROCESSOR, topicName + CDC_WRITER, WriterBuilder.start().getStreamDeltaWriterSCD2)
        case "scd4" => pipelineBuilder
          .setMergeSCD4Options(mergeSCD4Obj)
          .addAfter(topicName + CDC_READER, topicName + CDC_PROCESSOR, ProcessorBuilder.start().getProcessorStreamSCD4)
          .addAfter(topicName + CDC_PROCESSOR, topicName + CDC_WRITER, WriterBuilder.start().getStreamDeltaWriterSCD4)
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
    if(pipelineList.size()>0) {
      val es = Executors.newFixedThreadPool(pipelineList.size())
      es.invokeAll(pipelineList)
    }else{
      logger.info("Nothing to execute")
    }

  }

  /**
   * This function helps in extracting data from metadata config
   * @param r row
   * @return
   */

  def extractFields(r: Row): (String,String,mergeSCD1Options,mergeSCD2Options,mergeSCD4Options,String,String,Int,String) = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    val topicName = r.getAs("table_name").toString
    val productName = r.getAs("product_name").toString
    val partitionSize = r.getAs[Int]("partition_size")
    val pipelineDefId = r.getAs("pipeline_def_id").toString
    val jsonString = if(r.getAs("op_config") == null) "" else {r.getAs("op_config").toString }
    val outputStreamConf = parse(jsonString).extract[WriteStreamConfig]
    val SCDType = r.getAs("scd_type").toString
    val batchId = r.getAs("batch_id").toString

    val seq = scala.collection.mutable.ArrayBuffer[MergeBuilderLogicSCD1]()
    var logicSCD2: MergeBuilderLogicSCD2 = null
    if(SCDType == "scd1") {
      val mergeJsonArray = if (r.getAs("merge_cond") == null) "[]" else {r.getAs("merge_cond").toString}
      val ja = new JSONArray(mergeJsonArray)
      for (i <- 0 until ja.length()) {
        val jo = ja.getJSONObject(i).toString()
        seq += parse(jo).extract[MergeBuilderLogicSCD1]
      }
    } else if(SCDType == "scd2") {
      try {
        val mergeJsonObject = if (r.getAs("merge_cond") == null) "{}" else {r.getAs("merge_cond").toString}
        logicSCD2 = parse(mergeJsonObject).extract[MergeBuilderLogicSCD2]
      }
      catch {
        case e: Exception =>
          logger.error(s"Execption in $topicName while creating MergeBuilderLogicSCD2")
          logger.error(e.getMessage,e)
          throw new Exception(e)
      }
    }
    val logicSCD1 = if(seq.isEmpty) None else Some(seq)
    var joinKeys: Seq[String] = Seq[String]()
    if(r.getAs("join_key") == null) {
      val primaryKey = r.getAs("primary_key").toString
      joinKeys = Seq(primaryKey.split(",")).flatten.distinct
    } else{
      joinKeys = Seq(r.getAs("join_key").toString.split(",")).flatten
    }

    var partitionKeys: Option[Seq[String]] = None
    if(r.getAs("partition_id_col") != null) {
      val partitionKey = r.getAs("partition_id_col").toString
      partitionKeys = Some(Seq(partitionKey.split(",")).flatten.distinct)
    }

    var dedupKeys: Option[Seq[String]] = None
    if(r.getAs("updated_at_col") != null) {
      val dedupKey = r.getAs("updated_at_col").toString
      dedupKeys = Some(Seq(dedupKey.split(",")).flatten.distinct)
    }

    val extraJoinCond = if (r.getAs("extra_join_cond") == null) None else {Some(r.getAs("extra_join_cond").toString)}
    val mergeSCD1Obj = mergeSCD1Options(outputStreamConf,logicSCD1,joinKeys,partitionKeys,dedupKeys,extraJoinCond)
    val mergeSCD2Obj = mergeSCD2Options(outputStreamConf,logicSCD2,joinKeys,partitionKeys,dedupKeys,extraJoinCond)
    val mergeSCD4Obj = mergeSCD4Options(outputStreamConf,joinKeys,partitionKeys,dedupKeys,extraJoinCond)
    (topicName,productName,mergeSCD1Obj,mergeSCD2Obj,mergeSCD4Obj,SCDType,batchId,partitionSize,pipelineDefId)

  }

  /**
   * This function checkes the failed pipelines for given batch and re-triggers the pipelines which got failed.
   * @param spark
   * @param listBatchId
   */
  def rerunErrorBatch(spark: SparkSession,listBatchId:String): Unit ={
    logger.info("Rerun for Batch "+listBatchId)
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._
    val df =spark.read.table(s"""${ORCHESTRATION_DB}.${STATUS_TABLE_NAME}""").filter(s"batchId in ('${listBatchId}')")
    var runIDList=""
    val windowSpec  = Window.partitionBy("batchId").orderBy(col("lastUpdate").desc)
    val runId: Unit = df.withColumn("rank",rank().over(windowSpec)).filter("rank= '1'").select("runId").distinct().collectAsList().forEach(x=>{runIDList=runIDList.+(x.get(0).toString).+(",")})
    runIDList = runIDList.substring(0,runIDList.length-1).replaceAll(",","','")
    val sql = s"""select distinct pipelinedefid from ${ORCHESTRATION_DB}.${STATUS_TABLE_NAME} where runId in ('${runIDList}') and status='Error'""" //and status='Error'
    var pipelineList=""
    spark.sql(sql).collectAsList().forEach(x=>{pipelineList=pipelineList.+(x.get(0).toString).+(",")})
    if(pipelineList.isEmpty){
      logger.info("No pipeline found which got error for the latest batch")
      spark.emptyDataFrame.createOrReplaceTempView("metaTable")
      return
    }
    pipelineList=pipelineList.substring(0,pipelineList.length-1).replaceAll(",","','")
    logger.info("List of pipelines to run "+pipelineList)
    runPipeline(spark,pipelineList)
  }

  /**
   * This function runs the given set of pipeline.
   * @param spark
   * @param listOfPipeline
   */
  def runPipeline(spark: SparkSession,listOfPipeline:String): Unit ={
    logger.info("Pipelines to run :"+listOfPipeline)
    spark.sql(
      s"""
         |select distinct table_details.*,mapping.batch_id
         |from $ORCHESTRATION_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME table_details
         |inner join $ORCHESTRATION_DB.$PIPELINE_BATCH_MAP mapping on (table_details.pipeline_def_id = mapping.pipeline_def_id)
         |where table_details.pipeline_def_id in ('${listOfPipeline}')
         |""".stripMargin).distinct().createOrReplaceTempView("metaTable")
  }
  def defaultRun(spark: SparkSession,listOfBatchId:String): Unit ={
    logger.info("DefaultRun "+listOfBatchId)
    spark.sql(
      s"""
         |select distinct table_details.*,mapping.batch_id
         |from $ORCHESTRATION_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME table_details
         |inner join $ORCHESTRATION_DB.$PIPELINE_BATCH_MAP mapping on (table_details.pipeline_def_id = mapping.pipeline_def_id)
         |where mapping.batch_id in ('${listOfBatchId}')
         |""".stripMargin).createOrReplaceTempView("metaTable")
  }

}
