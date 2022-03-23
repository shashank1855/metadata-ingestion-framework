package com.databricks.genericPipelineOrchestrator.commons

import com.databricks.genericPipelineOrchestrator.Pipeline.PipelineNodeStatus
import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant.{ERROR_TABLE_NAME, FACT_TABLE_NAME, ORCHESTRATION_DB, STATUS_TABLE_NAME}
import com.databricks.genericPipelineOrchestrator.writter.{mergeSCD1Options, mergeSCD2Options, mergeSCD4Options}
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.log4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{current_date, current_timestamp, lit, max}

import java.time.LocalDateTime
import java.time.LocalDate
import java.util.concurrent.Callable
import scala.collection.mutable


object TaskOutputType extends Enumeration {
  val None, dataframe, temp_table = Value
}

/**
 * Task Class is the core entity of the Orchestration framework.Each Reader/Processor/Writer are task internally.
 */
abstract class Task extends Callable[Any]{
  var spark:SparkSession = null
  var taskName : String = null
  var taskOutputType :TaskOutputType.Value = TaskOutputType.None
  var taskOutputTableName :String = null
  var taskOutputDataFrames : mutable.Map[String,DataFrame] = null
  var inputDataframe: mutable.Map[String,DataFrame]  = null
  var pipelineName : String = null
  var pipeLineUid: String= null
  var runId: String = null
  var batchId:String = null
  var taskStatus: PipelineNodeStatus.Value = null;
  var taskFacts : List[Tuple3[String,String,String]] = null;
  var productName : String = null;
  var topicName : String = null;
  var mergeSCD1Options: mergeSCD1Options = null
  var mergeSCD2Options: mergeSCD2Options = null
  var logger : Logger = null;
  var mergeSCD4Options: mergeSCD4Options = null
  var tableName: String = null
  var partitionSize: Int = 0
  var pipelineDefId : String = null
  var readerOptions: mutable.Map[String,String] = null
  var inputPath: String = null
  /**
   * This function inserts the status of the task in the status table.
   * @param status current status of the pipeline
   */
  def updateAndStoreStatus(status: PipelineNodeStatus.Value) ={
    try {
      taskStatus = status;
      val localDateTime = LocalDateTime.now();
      val localDate = LocalDate.now()
      val insertSql = s"""INSERT INTO ${ORCHESTRATION_DB}.${STATUS_TABLE_NAME}  VALUES('${topicName}', '0','0', '0','${pipelineName}','${pipelineDefId}','${batchId}','${taskName}','${taskStatus}','${pipeLineUid}','${runId}','','','${localDateTime}','${localDate}')"""
      spark.sql(insertSql)
    }catch {
      case exception :Exception =>{
       logger.error(exception.getMessage,exception)
      }
    }
  }

  /**
   * This function stores the status of the task in-case of any exception
   * @param status
   * @param exception
   */
  def updateAndStoreStatus(status: PipelineNodeStatus.Value,exception: Exception): Unit ={
    try{
      taskStatus = status;
    val localDateTime= LocalDateTime.now();
    val localDate=LocalDate.now()
    spark.sql(s""" INSERT INTO ${ORCHESTRATION_DB}.${STATUS_TABLE_NAME} VALUES("${topicName}", '0','0', '0',"${pipelineName}","${pipelineDefId}","${batchId}","${taskName}","${taskStatus}","${pipeLineUid}","${runId}","${exception.getMessage}","${(ExceptionUtils.getStackTrace(exception)).replaceAll("\"","").toString}","${localDateTime}","${localDate}" )""")
  }catch {
    case exception :Exception =>{
      logger.error(exception.getMessage,exception)
    }
  }
  }

  /**
   * This function stores the stats of micro batch in-case of any exception.
   * @param dataFrame input dataframe on which exception has occurred.
   * @param updateTS the current timestamp
   * @param exception Exception
   */
  def foreachBatchStoreErrorStatus(batchDF:DataFrame, updateTS:Column,exception: Exception): Unit ={
    try{
    logger.info("Storing status for each batch"+taskName)
    var sql ="select concat_ws('|',topic,partition,offset"
    batchDF.createOrReplaceTempView("error_table")
    sql = sql + s""") as ${ErrorTableColumns.errorData.toString}  , now() ${ErrorTableColumns.insertTime.toString}  from error_table"""
    batchDF.sparkSession.sql(sql).write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(s"""${ORCHESTRATION_DB}.${ERROR_TABLE_NAME }""")

    val currentTs= current_timestamp()
    val statusDF = batchDF.groupBy("topic","partition")
      .agg(functions.min("offset").cast("long").as(StatusColumns.startOffset.toString),max("offset").cast("long").as(StatusColumns.endOffset.toString))
      .withColumn(StatusColumns.pipelineName.toString,lit(pipelineName))
      .withColumn(StatusColumns.pipelineDefId.toString,lit(pipelineDefId))
      .withColumn(StatusColumns.batchId.toString,lit(batchId))
      .withColumn(StatusColumns.instanceName.toString,lit(taskName))
      .withColumn(StatusColumns.status.toString,lit(PipelineNodeStatus.Error.toString))
      .withColumn(StatusColumns.pipelineId.toString,lit(pipeLineUid))
      .withColumn(StatusColumns.runId.toString,lit(runId))
      .withColumn(StatusColumns.errorMsg.toString,lit(exception.getMessage))
      .withColumn(StatusColumns.stackTrace.toString,lit(ExceptionUtils.getStackTrace(exception)))
      .withColumn(StatusColumns.lastUpdate.toString,lit(currentTs))
      .withColumn(StatusColumns.lastUpdateDate.toString,current_date())
    statusDF.show()
    statusDF.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(s"""${ORCHESTRATION_DB}.${STATUS_TABLE_NAME}""")
    logger.info("foreach batch logged error")
  }catch {
    case exception :Exception =>{
      logger.error(exception.getMessage,exception)
    }
  }
  }

  /**
   * In case of exception inside foreach batch,it stores all the records for that mini batch which got the exception
   * @param errorBatch mini batch for which exception occurred
   * @param updateTS
   */
  def foreachBatchLogError(errorBatch:DataFrame,updateTS:Column): Unit ={
    try {
      logger.info("inserting error records")
      var sql = "select concat_ws('|'"
      errorBatch.createOrReplaceTempView("error_table")
      errorBatch.columns.foreach(x => {
        sql = sql + "," + x.toString
      })
      sql = sql + s""") as ${ErrorTableColumns.errorData.toString}  , ${updateTS} ${ErrorTableColumns.insertTime.toString}  from error_table"""
      errorBatch.sparkSession.sql(sql).write.format("delta").mode("append").saveAsTable(s"""${ORCHESTRATION_DB}.${ERROR_TABLE_NAME}""")
    } catch {
    case exception :Exception =>{
      logger.error(exception.getMessage,exception)
    }
  }
  }

  /**
   * In case of Exception occurred outside of foreach batch,This function stores all the messages for that batch and exits
   * @param rawStream
   * @param exception
   */
  def streamingStoreStatusAndExit(rawStream: DataFrame, exception: Exception,exitFlag:Boolean): Unit={
   try {
     logger.info("inside store status and exit")
     rawStream.writeStream
       .format("delta")
       .outputMode("append")
       .option("mergeSchema", "true").foreachBatch((batchDF: DataFrame, batchId: Long) => {

       val timeStamp = current_timestamp()
       var sql = "select concat_ws('|',topic,partition,offset"
       batchDF.createOrReplaceTempView("error_table")

       sql = sql + s""") as ${ErrorTableColumns.errorData.toString}  , now() ${ErrorTableColumns.insertTime.toString}  from error_table"""
       batchDF.sparkSession.sql(sql).write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(s"""${ORCHESTRATION_DB}.${ERROR_TABLE_NAME}""")


       logger.info("Storing status for" + taskName)
       val currentTs = current_timestamp()
       val statusDF = batchDF.groupBy("topic", "partition")
         .agg(functions.min("offset").cast("long").as(StatusColumns.startOffset.toString), max("offset").cast("long").as(StatusColumns.endOffset.toString))
         .withColumn(StatusColumns.pipelineName.toString, lit(pipelineName))
         .withColumn(StatusColumns.pipelineDefId.toString, lit(pipelineDefId))
         .withColumn(StatusColumns.batchId.toString, lit(this.batchId))
         .withColumn(StatusColumns.instanceName.toString, lit(taskName))
         .withColumn(StatusColumns.status.toString, lit(PipelineNodeStatus.Error.toString))
         .withColumn(StatusColumns.pipelineId.toString, lit(pipeLineUid))
         .withColumn(StatusColumns.runId.toString, lit(runId))
         .withColumn(StatusColumns.errorMsg.toString, lit(exception.getMessage))
         .withColumn(StatusColumns.stackTrace.toString, lit(ExceptionUtils.getStackTrace(exception)))
         .withColumn(StatusColumns.lastUpdate.toString, lit(currentTs))
         .withColumn(StatusColumns.lastUpdateDate.toString, current_date())
       statusDF.show()
       statusDF.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(s"""${ORCHESTRATION_DB}.${STATUS_TABLE_NAME}""")

     }

     ).start().awaitTermination()
   } catch {
    case exception :Exception =>{
      logger.error(exception.getMessage,exception)
    }
  }
  }

  /**
   * For each batch it stores the status
   * @param rawDF
   * @param updateTS
   */
  def storeStatus(rawDF: DataFrame,updateTS:Column):Unit={
    try {
      logger.info("Storing status for" + taskName)
      val currentTs = updateTS
      val statusDF = rawDF.groupBy("topic", "partition")
        .agg(functions.min("offset").as(StatusColumns.startOffset.toString), max("offset").as(StatusColumns.endOffset.toString))
        .withColumn(StatusColumns.pipelineName.toString, lit(pipelineName))
        .withColumn(StatusColumns.pipelineDefId.toString, lit(pipelineDefId))
        .withColumn(StatusColumns.batchId.toString, lit(batchId))
        .withColumn(StatusColumns.instanceName.toString, lit(taskName))
        .withColumn(StatusColumns.status.toString, lit(PipelineNodeStatus.Running.toString))
        .withColumn(StatusColumns.pipelineId.toString, lit(pipeLineUid))
        .withColumn(StatusColumns.runId.toString, lit(runId))
        .withColumn(StatusColumns.errorMsg.toString, lit(""))
        .withColumn(StatusColumns.stackTrace.toString, lit(""))
        .withColumn(StatusColumns.lastUpdate.toString, lit(currentTs))
        .withColumn(StatusColumns.lastUpdateDate.toString,current_date())
      statusDF.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(s"${ORCHESTRATION_DB}.${STATUS_TABLE_NAME}")
    }catch {
      case exception: Exception =>{
        throw new Exception("Unable to insert data into status table:",exception)
      }
    }
    }

  /**
   * For each batch it stored the facts related to the batch
   * @param inputCount
   * @param outputCount
   * @param updateTS
   */
  def storeFacts(inputCount:Long,outputCount:Long,metricsDf: DataFrame,updateTS:Column):Unit={
      try {
        import org.apache.spark.sql.types._
        val data = Seq(
          Row(runId)
        )
        val schema = StructType(
          List(
            StructField(FactColumns.runId.toString, StringType, true)
          )
        )
        val df = spark.createDataFrame(
          spark.sparkContext.parallelize(data),
          schema
        )
        val factDF = df
          .withColumn(FactColumns.pipelineId.toString, lit(pipeLineUid))
          .withColumn(FactColumns.pipelineName.toString, lit(pipelineName))
          .withColumn(FactColumns.pipelineDefId.toString, lit(pipelineDefId))
          .withColumn(FactColumns.instanceName.toString, lit(taskName))
          .withColumn(FactColumns.topic.toString, lit(topicName))
          .withColumn(FactColumns.inputCount.toString, lit(inputCount))
          .withColumn(FactColumns.outputCount.toString, lit(outputCount))
          .withColumn(FactColumns.lastUpdate.toString, lit(updateTS))
          .withColumn(FactColumns.lastUpdateDate.toString, current_date())
        val finalDf = factDF.crossJoin(metricsDf)
        finalDf.write.format("delta").option("mergeSchema", "true").partitionBy(FactColumns.lastUpdateDate.toString,FactColumns.pipelineDefId.toString).mode("append").saveAsTable(s"${ORCHESTRATION_DB}.${FACT_TABLE_NAME}")
      }catch {
        case exception :Exception =>{
          logger.error(exception.getMessage,exception)
        }
      }

  }

  def getMetrics(tableName: String): DataFrame = {
    spark.sql(s"describe history $tableName limit 1").selectExpr("operationMetrics")
  }

}