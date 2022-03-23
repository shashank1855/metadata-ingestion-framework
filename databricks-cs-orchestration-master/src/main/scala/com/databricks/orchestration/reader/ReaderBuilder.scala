package com.databricks.genericPipelineOrchestrator.reader

import org.apache.log4j.Logger


class ReaderBuilder {
  val logger: Logger = Logger.getLogger(getClass)

  def getCsvReader(): Basereader={
    logger.info("CSV reader created")
    new CSVReaders()
  }

  def getSqlJdbcReader():Basereader={
    logger.info("sql reader created")
    new SqlJdbcReader()
  }

  def getKafkaReader():Basereader={
    logger.info("kafka reader created")
    new KafkaReader()
  }

  def getDeltaReader():Basereader={
    logger.info("Delta reader created")
    new DeltaReader()
  }

  def getDeltaStreamReader() = {
    logger.info("Delta stream reader created")
    new DeltaStreamReader()
  }

}


object ReaderBuilder
{
  def start(): ReaderBuilder ={
    new ReaderBuilder()
  }

}



