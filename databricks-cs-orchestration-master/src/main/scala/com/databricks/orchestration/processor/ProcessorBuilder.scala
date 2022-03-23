package com.databricks.genericPipelineOrchestrator.processor

import org.apache.log4j.Logger


class ProcessorBuilder {
  val logger: Logger = Logger.getLogger(getClass)

  def getProcessorStreamSCD1: BaseProcessor = {
    logger.info("BaseProcessor scd1 created")
    new ProcessorStreamSCD1()
  }

  def getProcessorDeltaStreamSCD1: BaseProcessor = {
    logger.info("BaseProcessor scd1 created")
    new ProcessorDeltaStreamSCD1()
  }

  def getProcessorStreamSCD2: BaseProcessor = {
    logger.info("BaseProcessor scd2 created")
    new ProcessorStreamSCD2()
  }

  def getProcessorDeltaStreamSCD2: BaseProcessor = {
    logger.info("BaseProcessor scd1 created")
    new ProcessorDeltaStreamSCD2()
  }

  def getProcessorStreamSCD4: BaseProcessor = {
    logger.info("BaseProcessor scd2 created")
    new ProcessorStreamSCD4()
  }

  def getProcessorDeltaStreamSCD4: BaseProcessor = {
    logger.info("BaseProcessor scd1 created")
    new ProcessorDeltaStreamSCD4()
  }
}


object ProcessorBuilder {
  def start(): ProcessorBuilder ={
    new ProcessorBuilder()
  }
}