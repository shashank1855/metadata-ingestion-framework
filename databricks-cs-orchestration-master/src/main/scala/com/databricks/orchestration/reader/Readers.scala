package com.databricks.genericPipelineOrchestrator.reader

import com.databricks.genericPipelineOrchestrator.Pipeline.PipelineNodeStatus
import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant._
import com.databricks.genericPipelineOrchestrator.commons.{OrchestrationConstant, TaskOutputType}
import com.databricks.genericPipelineOrchestrator.utility.Utility.getOffset
import org.apache.http.HttpHeaders
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.streaming.Trigger

import scala.collection.mutable


class SqlJdbcReader() extends Basereader{
  override def call()= {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started)

      val df = spark
        .read
        .format("jdbc")
        .options(this.readerOptions)
        .option("dbtable",this.tableName)
        .load()
        .limit(1000)

      val returnMap=new mutable.HashMap[String,DataFrame]()
      returnMap += PROCESSEDDF -> df
      returnMap += RAWDF -> df
      taskOutputDataFrames = returnMap
      updateAndStoreStatus(PipelineNodeStatus.Finished)
    } catch {
      case exception :Exception =>{
        logger.error(s"got exception in jdbc reader ${this.taskName}")
        logger.error(exception.getMessage,exception)
        updateAndStoreStatus(PipelineNodeStatus.Error,exception)
      }
    }
  }
}

class ParquetReader() extends Basereader{
  override def call()= {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started)

      val df = spark
        .read
        .format("parquet")
        .options(this.readerOptions)
        .load(this.inputPath)
        .limit(1000)

      val returnMap=new mutable.HashMap[String,DataFrame]()
      returnMap += PROCESSEDDF -> df
      returnMap += RAWDF -> df
      taskOutputDataFrames = returnMap
      updateAndStoreStatus(PipelineNodeStatus.Finished)
    } catch {
      case exception :Exception =>{
        logger.error(s"got exception in jdbc reader ${this.taskName}")
        logger.error(exception.getMessage,exception)
        updateAndStoreStatus(PipelineNodeStatus.Error,exception)
      }
    }
  }
}

class DeltaReader() extends Basereader{
  override def call(): Unit = {
    try {
      logger.info("Delta Reader Started")
      updateAndStoreStatus(PipelineNodeStatus.Started)
      val df = spark.readStream.format("delta").table(s"""${ORCHESTRATION_DB}.${STATUS_TABLE_NAME}""")
      var returnMap=new mutable.HashMap[String,DataFrame]()
      returnMap += (taskName-> df);
      df.createOrReplaceTempView("incomingDf")
      import org.apache.spark.sql.functions.current_timestamp
      val ts= current_timestamp();
      val r = new scala.util.Random
      r.nextInt(300)
      val factDf = spark.sql("select  topic as topic,"+r.nextInt()+" as partition,"+r.nextLong()+" as offset from incomingDf")
        .withColumn("runId",lit(runId))
        .withColumn("pipelineId",lit(pipeLineUid))
        .withColumn("pipelineName",lit(pipelineName))
        .withColumn("instanceName",lit(taskName))
        .withColumn("lastUpdate",lit(ts))
      ;
      logger.info("received data")
      returnMap += (PROCESSEDDF-> df);
      returnMap += (RAWDF-> factDf);
      taskOutputType=TaskOutputType.dataframe
      taskOutputDataFrames = returnMap

      updateAndStoreStatus(PipelineNodeStatus.Finished)
      logger.info("Delta Reader Completed")
    }catch {
      case e :Exception =>{
        updateAndStoreStatus(PipelineNodeStatus.Error,e)
        logger.info("got exception")
      }
    }

  }
}

class KafkaReader() extends Basereader{

  override def call(): mutable.HashMap[String, DataFrame] = {
    try {
      logger.info("Kafka reader started...")
      import org.apache.spark.sql.avro.functions._
      updateAndStoreStatus(PipelineNodeStatus.Started)
      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .option("subscribe",  topicName)
        .option("auto.offset.reset", "earliest")
        .option("kafka.security.protocol", "SASL_PLAINTEXT")
        .option("kafka.sasl.kerberos.service.name", "kafka")
        .option("auto.offset.reset", "earliest")
        .option("enable.auto.commit", "false")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger",100000)
        .option("startingOffsets",s"""${getOffset(spark,topicName,partitionSize)}""")
        .option("group.id", "use_a_separate_group_id_for_each_stream")
        .load()

      val t_df = df.select(
        from_avro(col("key"), topicName + "-key", AVRO_SCHEMA_REG_URL).as("key"),
        from_avro(col("value"), topicName + "-value", AVRO_SCHEMA_REG_URL).as("value")
      )

      logger.info("kafka reader ended.")
      var returnMap=new mutable.HashMap[String,DataFrame]()
      returnMap += PROCESSEDDF -> t_df
      returnMap += RAWDF -> df
      taskOutputDataFrames = returnMap
      updateAndStoreStatus(PipelineNodeStatus.Finished)

    } catch {
      case exception :Exception =>{
        logger.error(s"got exception in kafka reader ${this.taskName}")
        logger.error(exception.getMessage,exception)
        updateAndStoreStatus(PipelineNodeStatus.Error,exception)

      }
    }
    new mutable.HashMap[String, DataFrame]()
  }
}

class CSVReaders() extends Basereader {
  override def call()= {
    var returnMap = new mutable.HashMap[String, DataFrame]()
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started)
      returnMap += (taskName -> spark.sql("select * from "+ ORCHESTRATION_DB + "." + STATUS_TABLE_NAME +" order by last_update desc"));
      Thread.sleep(10000)
      var factList : List[(String, String, String)]= List()
      factList:+=(Tuple3("read count","0"+"","reading csv data  file name://"))
      factList:+=(Tuple3("write count","0","writing df data"))
      this.taskFacts = factList
      this.taskOutputType=TaskOutputType.dataframe;
      taskOutputDataFrames=returnMap
      updateAndStoreStatus(PipelineNodeStatus.Finished)
    }
    catch {
      case exception :Exception =>{
        updateAndStoreStatus(PipelineNodeStatus.Error,exception)
        logger.info("got exception")
      }
    }

  }
}

class DeltaStreamReader extends Basereader {
  override def call() = {

    try{
      logger.info("Delta Stream reader started...")
      updateAndStoreStatus(PipelineNodeStatus.Started)

      val df = spark
        .readStream
        .format("delta")
        .option("maxFilesPerTrigger",OrchestrationConstant.HISTORYLOADMAXFILEMAP.getOrElse(tableName,20))
        .table(this.productName + "." + this.tableName)

      logger.info("Delta Stream reader ended.")
      val returnMap=new mutable.HashMap[String,DataFrame]()
      returnMap += PROCESSEDDF -> df
      returnMap += RAWDF -> df
      taskOutputDataFrames = returnMap
      updateAndStoreStatus(PipelineNodeStatus.Finished)

    }
    catch {
      case exception :Exception =>{
        logger.info(s"got exception in reader ${this.taskName}")
        logger.info(exception)
        updateAndStoreStatus(PipelineNodeStatus.Error,exception)

      }
    }


  }
}