package com.databricks.genericPipelineOrchestrator.writter

import org.apache.log4j.Logger


class WriterBuilder {
  val logger = Logger.getLogger(getClass)

  /**
   *
   * Stream starts
   *
   *
   *
   *
   *
   */

  def getStreamDeltaWriterSCD1:BaseWritter={
    logger.info("stream delta writer SCD1 created")
    new StreamDeltaWriterSCD1()
  }

  def getStreamDeltaWriterSCD2:BaseWritter={
    logger.info("stream delta writer SCD2 created")
    new StreamDeltaWriterSCD2()
  }

  def getStreamDeltaWriterSCD4:BaseWritter={
    logger.info("stream delta writer SCD4 created")
    new StreamDeltaWriterSCD4()
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

  def getstreamDeltaWritterAppendSCD1: BaseWritter = {
    logger.info("stream delta writer append created")
    new StreamDeltaWritterAppendSCD1()
  }

  def getstreamDeltaWritterAppendSCD2: BaseWritter = {
    logger.info("stream delta writer append created")
    new StreamDeltaWritterAppendSCD2()
  }

  def getstreamDeltaWritterAppendSCD4: BaseWritter = {
    logger.info("stream delta writer append created")
    new StreamDeltaWritterAppendSCD4()
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

  def getDeltaWriterSCD1: BaseWritter = {
    logger.info("batch delta writer scd1 created")
    new deltaWriterSCD1()
  }

  def getDeltaWriterSCD2: BaseWritter = {
    logger.info("batch delta writer scd2 created")
    new deltaWriterSCD2()
  }

  def getDeltaWriterSCD4: BaseWritter = {
    logger.info("batch delta writer scd4 created")
    new deltaWriterSCD4()
  }


}


object WriterBuilder {
  def start(): WriterBuilder ={
    new WriterBuilder()
  }
}