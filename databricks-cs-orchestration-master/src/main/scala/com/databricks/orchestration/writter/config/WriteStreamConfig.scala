package com.databricks.genericPipelineOrchestrator.writter.config

import org.apache.log4j.Logger
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}

case class WriteStreamConfig(
  checkpointLocation: Option[String] = None,
  partitionBy: Option[Seq[String]] = None,
  triggerDuration: Option[String] = None,
  triggerMode: Option[String] = None,
  format: Option[String] = None,
  outputMode: Option[String] = None,
  txnVersion: Option[Int] = None,
  txnAppId: Option[Int] = None,
  extraOptions: Option[Map[String, String]] = None
) {
  val logger = Logger.getLogger(getClass)
  def applyOptions(writer: DataStreamWriter[_]): Unit = {
    checkpointLocation match {
      case Some(location) => writer.option("checkpointLocation", location)
      case None =>
    }

    outputMode match {
      case Some(outputMode) => writer.outputMode(outputMode)
      case None =>
    }

    partitionBy match {
      case Some(partitionBy) => writer.partitionBy(partitionBy: _*)
      case None =>
    }

    format match {
      case Some(format) => writer.format(format)
      case None =>
    }

    txnVersion match {
      case Some(txnVersion) => writer.option("txnVersion", txnVersion)
      case None =>
    }

    txnAppId match {
      case Some(txnAppId) => writer.option("txnAppId", txnAppId)
      case None =>
    }

    (triggerMode, triggerDuration) match {
      case (Some("ProcessingTime"), Some(duration)) => writer.trigger(Trigger.ProcessingTime(duration))
      case (Some("Once"), _) => writer.trigger(Trigger.Once())
      case (Some("Continuous"), Some(duration)) => writer.trigger(Trigger.Continuous(duration))
      case _ =>
        logger.info(
          "no triggerMode was passed or trigger sent is invalid. writer will be returned with default trigger mode"
        )
        writer
    }

    extraOptions match {
      case Some(options) => writer.options(options)
      case None =>
    }
  }
}
