package com.databricks.genericPipelineOrchestrator.writter

import com.databricks.genericPipelineOrchestrator.writter.config.WriteStreamConfig
import org.apache.spark.sql.DataFrame


object ProcessOutputStream {

  /**
   * This method helps to write stream directly to a atble
   * @param mode Should be Some(test) for testing and None for production
   * @param inputStreamDF Is the input stream DF which has to be written to a table
   * @param targetTableName Is the target table name where data has to stored
   * @param outputConf Is a case class object with all the required options to be applied on the inputstream DF
   */

  def processOutputStream(
                           mode: Option[String],
                           inputStreamDF: DataFrame,
                           targetTableName: String,
                           outputConf: WriteStreamConfig
                         ): Unit = {
    val writer = inputStreamDF.writeStream

    val conf = Option(outputConf)
    Option(outputConf).orElse(conf) match {
      case Some(config) => config.applyOptions(writer)
      case None =>
    }

    val query = writer.queryName(targetTableName).toTable(targetTableName)
    mode match {
      case Some("test") => query.processAllAvailable()
      case None => query.awaitTermination()
    }



  }

  /**
   * This function helps in writing input stream after applying the foreach batch function
   * @param mode Should be Some(test) for testing and None for production
   * @param inputStreamDf Is the input stream DF on which foreachbtahc function has to be applied
   * @param targetTableName This table name will be used in foreachbatch function
   * @param func is the foreachbacth to be applied on inputstream
   * @param outputConf Is a case class object with all the required options to be applied on the inputstream DF
   */

  def processOutputStreamBatch(
                                mode: Option[String],
                                inputStreamDf: DataFrame,
                                targetTableName: String,
                                func: (DataFrame, Long) => Unit,
                                outputConf: WriteStreamConfig
                              ): Unit = {

    val writer = inputStreamDf.writeStream
    val conf = Option(outputConf)

    Option(outputConf).orElse(conf) match {
      case Some(config) => config.applyOptions(writer)
      case None =>
    }

    val query = writer
      .queryName(targetTableName)
      .foreachBatch(func)
      .start()

    mode match {
      case Some("test") => query.processAllAvailable()
      case None => query.awaitTermination()
    }

  }

}
