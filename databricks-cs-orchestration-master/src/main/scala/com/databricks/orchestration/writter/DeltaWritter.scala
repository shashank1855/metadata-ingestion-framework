package com.databricks.genericPipelineOrchestrator.writter

import com.databricks.genericPipelineOrchestrator.Pipeline.PipelineNodeStatus
import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant._
import com.databricks.genericPipelineOrchestrator.utility.Utility.{writeSCD1, writeSCD2, writeSCD4, writeStreamSCD1, writeStreamSCD2, writeStreamSCD4}
import com.databricks.genericPipelineOrchestrator.writter.config.WriteStreamConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}
import com.databricks.genericPipelineOrchestrator.utility._

/**
 *
 * Stream starts
 *
 *
 *
 *
 *
 */

class StreamDeltaWriterSCD1 extends BaseWritter {
  override def call(): Any = {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started)
      logger.info("INFO: scd1 writer started")
      val rawDf = this.inputDataframe(RAWDF)
      val inputStream = this.inputDataframe(PROCESSEDDF)
      val productName = this.productName
      val topicName = this.topicName
      val tableName = this.tableName
      val joinKeys = this.mergeSCD1Options.joinKeys
      val partitionKeys = this.mergeSCD1Options.partitionKeys
      val dedupKeys = this.mergeSCD1Options.dedupKeys
      val MergeBuilderLogic = this.mergeSCD1Options.MergeBuilderLogic
      val extraJoinCond = this.mergeSCD1Options.extraJoinCondition
      val outputStreamConf: WriteStreamConfig = this.mergeSCD1Options.outputStreamConf

      val queueDf = inputStream.select(struct("*").as("inputStream"), lit(null).as("rawDf"))
        .union(rawDf.select(lit(null).as("inputStream"), struct("*").as("rawDf")))

      //calling the processOutputStreamBatch to apply foreachbatch on mini batch
      ProcessOutputStream.processOutputStreamBatch(
        None,
        queueDf,
        tableName,
        (batchDf: Dataset[Row], batchId: Long) =>
          writeStreamSCD1(batchDf
            , s"${productName}_cdc" + "." + tableName
            , joinKeys
            , MergeBuilderLogic
            , extraJoinCond
            , this
            , partitionKeys
            , dedupKeys),
        outputStreamConf
      )

    }

    catch {
      case exception: Exception => {
        logger.error("got exception in " + taskName)
        logger.error(exception.getMessage,exception)
        logger.error(s"INFO : Inside StreamDeltaWritterSCD1 catch")
        this.streamingStoreStatusAndExit(this.inputDataframe(RAWDF),exception,true)
       // updateAndStoreStatus(PipelineNodeStatus.Error, exception)

      }

    }
  }
}


class StreamDeltaWriterSCD2 extends BaseWritter {
  override def call(): Any = {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started)
      logger.info("INFO: scd2 writer started")
      val rawDf = this.inputDataframe(RAWDF)
      val inputStream = this.inputDataframe(PROCESSEDDF)
      val productName = this.productName
      val topicName = this.topicName
      val tableName = this.tableName
      val joinKeys = this.mergeSCD2Options.joinKeys
      val partitionKeys = this.mergeSCD2Options.partitionKeys
      val dedupKeys = this.mergeSCD2Options.dedupKeys
      val outputStreamConf: WriteStreamConfig = this.mergeSCD2Options.outputStreamConf
      val MergeBuilderLogic: MergeBuilderLogicSCD2 = this.mergeSCD2Options.MergeBuilderLogic
      val extraJoinCond = this.mergeSCD2Options.extraJoinCondition

      val queueDf = inputStream.select(struct("*").as("inputStream"), lit(null).as("rawDf"))
        .union(rawDf.select(lit(null).as("inputStream"), struct("*").as("rawDf")))

      ProcessOutputStream.processOutputStreamBatch(
        None,
        queueDf,
        tableName,
        (batchDf: Dataset[Row], batchId: Long) =>
          writeStreamSCD2(batchDf
            , s"${productName}_cdc" + "." + tableName
            , joinKeys
            , MergeBuilderLogic
            , extraJoinCond
            , this
            , partitionKeys
            , dedupKeys),
        outputStreamConf
      )

    }
    catch {
      case e: Exception => {
        logger.error("got exception in " + taskName)
        logger.error(e.getMessage,e)
        this.streamingStoreStatusAndExit(this.inputDataframe(RAWDF),e,true)
      }

    }
  }
}

class StreamDeltaWriterSCD4 extends BaseWritter {
  override def call(): Any = {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started)
      println("INFO: SCD4 writer started")
      val rawDf = this.inputDataframe(RAWDF)
      val inputStream = this.inputDataframe(PROCESSEDDF)
      val productName = this.productName
      val topicName = this.topicName
      val tableName = this.tableName
      val joinKeys = this.mergeSCD4Options.joinKeys
      val partitionKeys = this.mergeSCD4Options.partitionKeys
      val dedupKeys = this.mergeSCD4Options.dedupKeys
      val extraJoinCondition = this.mergeSCD4Options.extraJoinCondition
      val outputStreamConf: WriteStreamConfig = this.mergeSCD4Options.outputStreamConf
      val spark = this.spark

      val queueDf = inputStream.select(struct("*").as("inputStream"), lit(null).as("rawDf"))
        .union(rawDf.select(lit(null).as("inputStream"), struct("*").as("rawDf")))

      ProcessOutputStream.processOutputStreamBatch(
        None,
        queueDf,
        tableName,
        (batchDf: Dataset[Row], batchId: Long) =>
          writeStreamSCD4(spark
            , batchDf
            , s"${productName}_cdc"  + "." + tableName
            , joinKeys
            , extraJoinCondition
          , this
          , partitionKeys
          , dedupKeys),
        outputStreamConf
      )

    }
    catch {
      case e: Exception => {
        println("got exception in " + taskName)
        logger.error(e.getMessage,e)
        this.streamingStoreStatusAndExit(this.inputDataframe(RAWDF),e,true)

      }
    }
  }
}


/**
 *
 * Batch starts
 *
 *
 *
 *
 *
 */

class deltaWriterSCD1 extends BaseWritter {
  override def call(): Any = {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started)
      logger.info("INFO: scd1 writer started")
      val rawDf = this.inputDataframe(RAWDF)
      val inputBatch = this.inputDataframe(PROCESSEDDF)
      val productName = this.productName
      val topicName = this.topicName
      val tableName = this.tableName
      val joinKeys = this.mergeSCD1Options.joinKeys
      val partitionKeys = this.mergeSCD1Options.partitionKeys
      val dedupKeys = this.mergeSCD1Options.dedupKeys
      val MergeBuilderLogic = this.mergeSCD1Options.MergeBuilderLogic
      val extraJoinCond = this.mergeSCD1Options.extraJoinCondition

      writeSCD1(this.spark
        , inputBatch
        , s"${productName}_cdc_db" + "." + tableName
        , joinKeys
        , MergeBuilderLogic
        , extraJoinCond
        , partitionKeys
        , dedupKeys
        , Some(this))

      val currentTimestamp = current_timestamp()
      updateAndStoreStatus(PipelineNodeStatus.Finished)
      val metricsDf = this.getMetrics(s"${productName}_cdc" + "." + tableName)
      storeFacts(rawDf.count(),inputBatch.count(),metricsDf,currentTimestamp)

    }

    catch {
      case exception: Exception => {
        logger.error("got exception in " + taskName)
        println(exception)
        logger.error(exception.getMessage,exception)
        updateAndStoreStatus(PipelineNodeStatus.Error, exception)

      }

    }
  }
}

class deltaWriterSCD2 extends BaseWritter {
  override def call(): Any = {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started)
      logger.info("INFO: scd2 writer started")
      val rawDf = this.inputDataframe(RAWDF)
      val inputBatch = this.inputDataframe(PROCESSEDDF)
      val productName = this.productName
      val topicName = this.topicName
      val tableName = this.tableName
      val joinKeys = this.mergeSCD2Options.joinKeys
      val partitionKeys = this.mergeSCD2Options.partitionKeys
      val dedupKeys = this.mergeSCD2Options.dedupKeys
      val outputStreamConf: WriteStreamConfig = this.mergeSCD2Options.outputStreamConf
      val MergeBuilderLogic: MergeBuilderLogicSCD2 = this.mergeSCD2Options.MergeBuilderLogic
      val extraJoinCond = this.mergeSCD2Options.extraJoinCondition



      writeSCD2(this.spark
        , inputBatch
        , s"${productName}_cdc_db" + "." + tableName
        , joinKeys
        , MergeBuilderLogic
        , extraJoinCond
        , partitionKeys
        , dedupKeys
        , Some(this))

      val currentTimestamp = current_timestamp()
      updateAndStoreStatus(PipelineNodeStatus.Finished)
      val metricsDf = this.getMetrics(s"${productName}_cdc" + "." + tableName)
      storeFacts(rawDf.count(),inputBatch.count(),metricsDf,currentTimestamp)

    }
    catch {
      case exception: Exception => {
        logger.error("got exception in " + taskName)
        println(exception)
        logger.error(exception.getMessage,exception)
        updateAndStoreStatus(PipelineNodeStatus.Error, exception)
      }
    }
  }
}

class deltaWriterSCD4 extends BaseWritter {
  override def call(): Any = {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started)
      println("INFO: SCD4 writer started")
      val rawDf = this.inputDataframe(RAWDF)
      val inputBatch = this.inputDataframe(PROCESSEDDF)
      val productName = this.productName
      val topicName = this.topicName
      val tableName = this.tableName
      val joinKeys = this.mergeSCD4Options.joinKeys
      val partitionKeys = this.mergeSCD4Options.partitionKeys
      val dedupKeys = this.mergeSCD4Options.dedupKeys
      val extraJoinCondition = this.mergeSCD4Options.extraJoinCondition
      val outputStreamConf: WriteStreamConfig = this.mergeSCD4Options.outputStreamConf
      val spark = this.spark



      writeSCD4(spark
            , inputBatch
            , s"${productName}_cdc"  + "." + tableName
            , joinKeys
            , extraJoinCondition
            , partitionKeys
            , dedupKeys
            , Some(this))

    }
    catch {
      case e: Exception => {
        println("got exception in " + taskName)
        logger.error(e.getMessage,e)
        this.streamingStoreStatusAndExit(this.inputDataframe(RAWDF),e,true)

      }
    }
  }
}

/**
 *
 * History Stream starts
 *
 *
 *
 *
 *
 */

class StreamDeltaWritterAppendSCD1 extends BaseWritter {
  override def call(): Any =  {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started)
      println("INFO: append stream writer started")

      val inputStream = this.inputDataframe(PROCESSEDDF)

      val partitionBy = this.mergeSCD1Options.partitionKeys

      //create output stream config
      val outputStreamConf : WriteStreamConfig = WriteStreamConfig(
        format = Some("delta"),
        partitionBy = partitionBy,
        outputMode = Some("append"),
        checkpointLocation = Some(s"${HISTORYLOADCHECKPOINTLOC}/${this.tableName}"),
        triggerMode = Some("ProcessingTime"),
        triggerDuration = Some("2 minutes"))

      //calling the processOutputStreamBatch to apply foreachbatch on mini batch
      ProcessOutputStream.processOutputStream(
        None,
        inputStream,
        s"${this.productName}_cdc" + "." + this.tableName,
        outputStreamConf
      )

    }
    catch {
      case e: Exception => {
        println("got exception in " + taskName)
        logger.error(e.getMessage,e)
        updateAndStoreStatus(PipelineNodeStatus.Error, e)

      }
    }

  }
}

class StreamDeltaWritterAppendSCD2 extends BaseWritter {
  override def call(): Any =  {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started)
      println("INFO: append stream writer started")

      val inputStream = this.inputDataframe(PROCESSEDDF)

      val partitionBy = this.mergeSCD2Options.partitionKeys

      //create output stream config
      val outputStreamConf : WriteStreamConfig = WriteStreamConfig(
        format = Some("delta"),
        partitionBy = partitionBy,
        outputMode = Some("append"),
        checkpointLocation = Some(s"${HISTORYLOADCHECKPOINTLOC}/${this.tableName}"),
        triggerMode = Some("ProcessingTime"),
        triggerDuration = Some("2 minutes"))

      //calling the processOutputStreamBatch to apply foreachbatch on mini batch
      ProcessOutputStream.processOutputStream(
        None,
        inputStream,
        s"${this.productName}_cdc" + "." + this.tableName,
        outputStreamConf
      )

    }
    catch {
      case e: Exception => {
        println("got exception in " + taskName)
        logger.error(e.getMessage,e)
        updateAndStoreStatus(PipelineNodeStatus.Error, e)

      }
    }

  }
}


class StreamDeltaWritterAppendSCD4 extends BaseWritter {
  override def call(): Any =  {
    try {
      updateAndStoreStatus(PipelineNodeStatus.Started)
      println("INFO: append stream writer started")

      val inputStream = this.inputDataframe(PROCESSEDDF)

      val partitionBy = this.mergeSCD4Options.partitionKeys

      //create output stream config
      val outputStreamConf : WriteStreamConfig = WriteStreamConfig(
        format = Some("delta"),
        partitionBy = partitionBy,
        outputMode = Some("append"),
        checkpointLocation = Some(s"${HISTORYLOADCHECKPOINTLOC}/${this.tableName}"),
        triggerMode = Some("ProcessingTime"),
        triggerDuration = Some("2 minutes"))

      //calling the processOutputStreamBatch to apply foreachbatch on mini batch
      ProcessOutputStream.processOutputStream(
        None,
        inputStream,
        s"${this.productName}_cdc" + "." + this.tableName,
        outputStreamConf
      )

    }
    catch {
      case e: Exception => {
        println("got exception in " + taskName)
        logger.error(e.getMessage,e)
        updateAndStoreStatus(PipelineNodeStatus.Error, e)

      }
    }

  }
}

