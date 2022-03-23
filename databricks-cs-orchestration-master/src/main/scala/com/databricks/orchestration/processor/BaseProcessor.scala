package com.databricks.genericPipelineOrchestrator.processor

import com.databricks.genericPipelineOrchestrator.Pipeline.PipelineNodeStatus
import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant._
import com.databricks.genericPipelineOrchestrator.commons.{OrchestrationConstant, Task, TaskOutputType}
import com.databricks.genericPipelineOrchestrator.utility.Utility._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, lit, regexp_replace, row_number, timestamp_seconds}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

abstract class BaseProcessor extends  Task{
}

/**
 *
 *
 *
 * Stream Processor starts
 *
 *
 */

class ProcessorStreamSCD1() extends BaseProcessor {
  override def call(): mutable.HashMap[String, DataFrame] = {
    val returnMap = new mutable.HashMap[String, DataFrame]()
    try {

      updateAndStoreStatus(PipelineNodeStatus.Started)
      println("INFO: Processer started")
      val rawDf = this.inputDataframe(RAWDF)
      val inputStream = this.inputDataframe(PROCESSEDDF)
      val productName = this.productName
      val joinKeys = extractJoinKeys(this.mergeSCD1Options.joinKeys)

      val upsertDF = inputStream.filter(inputStream("value.op") === "c" || inputStream("value.op") === "u")
        .withColumn("row_active",lit(true))
        .withColumn("deleted_flag",lit(false))
        .selectExpr( "value.after.*","row_active","deleted_flag","value.source.db","value.source.server_id")


      val deleteDF = inputStream.filter(inputStream("value.op") === "d")
        .withColumn("row_active",lit(false))
        .withColumn("deleted_flag",lit(true))
        .selectExpr( "value.before.*","row_active","deleted_flag","value.source.db","value.source.server_id")


      val shardMappingDf = spark.read.table(s"${ORCHESTRATION_DB}.${INTERNAL_SHARD_MAPPINGS_TABLE_NAME}")
      val updatesDF = (upsertDF.union(deleteDF)).alias("stream").join(shardMappingDf.as("batch")
        , col("stream.db") === col("batch.database_name")
        && col("stream.server_id") === col("batch.server_id"), "inner")
        .selectExpr("stream.*","batch.shard_name")

      val piiDf = spark.read.table(s"$ORCHESTRATION_DB.$PII_COLUMN_DETAILS_TABLE_NAME").filter(s"product_name ='$productName'")

      //Omit columns
      val omittedColumns = spark.read.table(s"$ORCHESTRATION_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME").filter(s"product_name ='${productName}' and table_name='${this.tableName}'").selectExpr("coalesce(omitted_cols,'')").collect()(0)(0).toString
      val omittedColumnsList = omittedColumns.split(",").distinct
      val omittedDf = dropIt(updatesDF, omittedColumnsList: _*)
      val inputStreamColumns = omittedDf.columns

      //Create hash key out of all columns for join
      val hashJoinColDf = hashIt(omittedDf,"hashed_jk",NUMBITS256,inputStreamColumns:_*)

      //anonymize complete columns
      val completeAnonymizationColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and anonymization_flag='complete'").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val completeAnonymizeColumnsList = identifyColumns(inputStreamColumns, completeAnonymizationColumns).distinct
      val completeHashedDf = hashItComplete(hashJoinColDf, NUMBITS256, completeAnonymizeColumnsList: _*)

      //anonymize partial columns
      val partialAnonymizationColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and anonymization_flag='partial'").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val partialAnonymizeColumnsList = identifyColumns(inputStreamColumns, partialAnonymizationColumns).distinct
      val partialHashedDf = hashItPartial(completeHashedDf, partialAnonymizeColumnsList: _*)

      //encrypt columns
      val encryptionColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and encryption_flag=true").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val encryptionColumnsList = identifyColumns(inputStreamColumns, encryptionColumns).distinct
      val encryptedDf = encryptIt(partialHashedDf, encryptionColumnsList: _*)

      val transformedDf = hashNumericalIt(encryptedDf,"grouping_jk",joinKeys:_*)

      val ephocColumnList = List("created_at","updated_at")
      val finalTransformedDf = ephocColumnList.foldLeft(transformedDf) {
        case(tmpdf, columnName) =>castEphocToTimestamp(tmpdf,columnName)
      }

      finalTransformedDf.withColumn("insert_at",lit(current_timestamp())).printSchema()


      logger.info(s"omittedColumnsList is ${omittedColumnsList.mkString("Array(", ", ", ")")}")
      logger.info(s"completeAnonymizeColumnsList is ${completeAnonymizeColumnsList.mkString("Array(", ", ", ")")}")
      logger.info(s"partialAnonymizationColumns is ${partialAnonymizationColumns.mkString("Array(", ", ", ")")}")
      logger.info(s"encryptionColumnsList is ${encryptionColumnsList.mkString("Array(", ", ", ")")}")

      returnMap += PROCESSEDDF -> finalTransformedDf.withColumn("insert_at",lit(current_timestamp()))
      returnMap += RAWDF -> rawDf
      taskOutputDataFrames = returnMap
      println("INFO: Processer ended")
      updateAndStoreStatus(PipelineNodeStatus.Finished)
      returnMap
    }
    catch {
      case e :Exception =>{
        println(e)
        updateAndStoreStatus(PipelineNodeStatus.Error,e)
        println("got exception")
      }
    }
    returnMap

  }
}


class ProcessorStreamSCD2() extends BaseProcessor {
  override def call(): Any = {
    val returnMap = new mutable.HashMap[String, DataFrame]()
    try {

      updateAndStoreStatus(PipelineNodeStatus.Started)
      println("INFO: Processor started")
      val rawDf = this.inputDataframe(RAWDF)
      val inputStream = this.inputDataframe(PROCESSEDDF)
      val productName = this.productName
      val joinKeys = extractJoinKeys(this.mergeSCD2Options.joinKeys)

      val upsertDF = inputStream.filter(inputStream("value.op") === "c" || inputStream("value.op") === "u")
        .withColumn("row_active",lit(true))
        .withColumn("deleted_flag",lit(false).cast(BooleanType))
        .selectExpr( "value.after.*","row_active","deleted_flag","value.source.db","value.source.server_id")


      val deleteDF = inputStream.filter(inputStream("value.op") === "d")
        .withColumn("row_active",lit(false))
        .withColumn("deleted_flag",lit(true).cast(BooleanType))
        .selectExpr( "value.before.*","row_active","deleted_flag","value.source.db","value.source.server_id")


      val shardMappingDf = spark.read.table(s"${ORCHESTRATION_DB}.${INTERNAL_SHARD_MAPPINGS_TABLE_NAME}")
      val updatesDF = (upsertDF.union(deleteDF)).alias("stream").join(shardMappingDf.as("batch")
        , col("stream.db") === col("batch.database_name")
          && col("stream.server_id") === col("batch.server_id"), "inner")
        .selectExpr("stream.*","batch.shard_name")

      val piiDf = spark.read.table(s"$ORCHESTRATION_DB.$PII_COLUMN_DETAILS_TABLE_NAME").filter(s"product_name ='$productName'")

      //Omit columns
      val omittedColumns = spark.read.table(s"$ORCHESTRATION_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME").filter(s"product_name ='${productName}' and table_name='${this.tableName}'").selectExpr("coalesce(omitted_cols,'')").collect()(0)(0).toString
      val omittedColumnsList = omittedColumns.split(",").distinct
      val omittedDf = dropIt(updatesDF, omittedColumnsList: _*)
      val inputStreamColumns = omittedDf.columns


      //anonymize complete columns
      val completeAnonymizationColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and anonymization_flag='complete'").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val completeAnonymizeColumnsList = identifyColumns(inputStreamColumns, completeAnonymizationColumns).distinct
      val completeHashedDf = hashItComplete(omittedDf, NUMBITS256, completeAnonymizeColumnsList: _*)

      //anonymize partial columns
      val partialAnonymizationColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and anonymization_flag='partial'").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val partialAnonymizeColumnsList = identifyColumns(inputStreamColumns, partialAnonymizationColumns).distinct
      val partialHashedDf = hashItPartial(completeHashedDf, partialAnonymizeColumnsList: _*)

      //encrypt columns
      val encryptionColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and encryption_flag=true").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val encryptionColumnsList = identifyColumns(inputStreamColumns, encryptionColumns).distinct
      val encrypteddDf = encryptIt(partialHashedDf, encryptionColumnsList: _*)

      val transformedDf = joinKeys.foldLeft(encrypteddDf) {
        case (tmpdf, columnName) => tmpdf.withColumn(columnName.toString(), coalesce(columnName,lit("n/a")))
      }

      val transformedDf_1 = hashNumericalIt(transformedDf,"grouping_jk",joinKeys:_*)

      val ephocColumnList = List("created_at","updated_at")
      val finalTransformedDf = ephocColumnList.foldLeft(transformedDf_1) {
        case(tmpdf, columnName) =>castEphocToTimestamp(tmpdf,columnName)
      }

      finalTransformedDf
        .withColumn("current_flag",lit(true).cast(BooleanType))
        .withColumn("expiry_at",lit(null).cast(LongType))
        .printSchema()

      returnMap += PROCESSEDDF -> finalTransformedDf
        .withColumn("current_flag",lit(true).cast(BooleanType))
        .withColumn("expiry_at",lit(null).cast(LongType))
      returnMap += RAWDF -> rawDf
      taskOutputDataFrames = returnMap
      println("INFO: Processer ended")
      updateAndStoreStatus(PipelineNodeStatus.Finished)

    }
    catch {
      case e: Exception => {
        println(e)
        updateAndStoreStatus(PipelineNodeStatus.Error, e)
        println("got exception")
      }
    }

  }
}


class ProcessorStreamSCD4() extends BaseProcessor {
  override def call(): Any = {
    val returnMap = new mutable.HashMap[String, DataFrame]()
    try {

      updateAndStoreStatus(PipelineNodeStatus.Started)
      println("INFO: Processor started")
      val rawDf = this.inputDataframe(RAWDF)
      val inputStream = this.inputDataframe(PROCESSEDDF)
      val productName = this.productName
      val joinKeys = extractJoinKeys(this.mergeSCD4Options.joinKeys)

      val upsertDF = inputStream.filter(inputStream("value.op") === "c" || inputStream("value.op") === "u")
        .withColumn("row_active",lit(true))
        .withColumn("deleted_flag",lit(false).cast(BooleanType))
        .selectExpr( "value.after.*","row_active","deleted_flag","value.source.db","value.source.server_id")


      val deleteDF = inputStream.filter(inputStream("value.op") === "d")
        .withColumn("row_active",lit(false))
        .withColumn("deleted_flag",lit(true).cast(BooleanType))
        .selectExpr( "value.before.*","row_active","deleted_flag","value.source.db","value.source.server_id")


      val shardMappingDf = spark.read.table(s"${ORCHESTRATION_DB}.${INTERNAL_SHARD_MAPPINGS_TABLE_NAME}")
      val updatesDF = (upsertDF.union(deleteDF)).alias("stream").join(shardMappingDf.as("batch")
        , col("stream.db") === col("batch.database_name")
          && col("stream.server_id") === col("batch.server_id"), "inner")
        .selectExpr("stream.*","batch.shard_name")

      val piiDf = spark.read.table(s"$ORCHESTRATION_DB.$PII_COLUMN_DETAILS_TABLE_NAME").filter(s"product_name ='$productName'")

      //Omit columns
      val omittedColumns = spark.read.table(s"$ORCHESTRATION_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME").filter(s"product_name ='${productName}' and table_name='${this.tableName}'").selectExpr("coalesce(omitted_cols,'')").collect()(0)(0).toString
      val omittedColumnsList = omittedColumns.split(",").distinct
      val omittedDf = dropIt(updatesDF, omittedColumnsList: _*)
      val inputStreamColumns = omittedDf.columns


      //anonymize complete columns
      val completeAnonymizationColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and anonymization_flag='complete'").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val completeAnonymizeColumnsList = identifyColumns(inputStreamColumns, completeAnonymizationColumns).distinct
      val completeHashedDf = hashItComplete(omittedDf, NUMBITS256, completeAnonymizeColumnsList: _*)

      //anonymize partial columns
      val partialAnonymizationColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and anonymization_flag='partial'").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val partialAnonymizeColumnsList = identifyColumns(inputStreamColumns, partialAnonymizationColumns).distinct
      val partialHashedDf = hashItPartial(completeHashedDf, partialAnonymizeColumnsList: _*)

      //encrypt columns
      val encryptionColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and encryption_flag=true").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val encryptionColumnsList = identifyColumns(inputStreamColumns, encryptionColumns).distinct
      val encrypteddDf = encryptIt(partialHashedDf, encryptionColumnsList: _*)

      val transformedDf_1 = hashNumericalIt(encrypteddDf,"grouping_jk",joinKeys:_*)

      val ephocColumnList = List("created_at","updated_at")
      val finalTransformedDf = ephocColumnList.foldLeft(transformedDf_1) {
        case(tmpdf, columnName) =>castEphocToTimestamp(tmpdf,columnName)
      }

      finalTransformedDf
        .printSchema()

      returnMap += PROCESSEDDF -> finalTransformedDf
      returnMap += RAWDF -> rawDf
      taskOutputDataFrames = returnMap
      println("INFO: Processer ended")
      updateAndStoreStatus(PipelineNodeStatus.Finished)

    }
    catch {
      case e: Exception => {
        println(e)
        updateAndStoreStatus(PipelineNodeStatus.Error, e)
        println("got exception")
      }
    }

  }
}

/**
 *
 *
 *
 * History/Batch Processor starts
 *
 *
 */

class ProcessorDeltaStreamSCD1() extends BaseProcessor {
  override def call(): mutable.HashMap[String, DataFrame] = {
    val returnMap = new mutable.HashMap[String, DataFrame]()
    try {

      updateAndStoreStatus(PipelineNodeStatus.Started)
      println("INFO: Processer started")
      val inputStream = this.inputDataframe(PROCESSEDDF)
      val rawDf = this.inputDataframe(RAWDF)
      val productName = this.productName
      val joinKeys = extractJoinKeys(this.mergeSCD1Options.joinKeys)


      val updatesDF = inputStream

      val piiDf = spark.read.table(s"$ORCHESTRATION_DB.$PII_COLUMN_DETAILS_TABLE_NAME").filter(s"product_name ='$productName'")

      //Omit columns
      val omittedColumns = spark.read.table(s"$ORCHESTRATION_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME").filter(s"product_name ='${productName}' and table_name='${this.tableName}'").selectExpr("coalesce(omitted_cols,'')").collect()(0)(0).toString
      val omittedColumnsList = omittedColumns.split(",").distinct
      val omittedDf = dropIt(updatesDF, omittedColumnsList: _*)
      val inputStreamColumns = omittedDf.columns

      //Create hash key out of all columns for join
      val hashJoinColDf = hashIt(omittedDf,"hashed_jk",NUMBITS256,inputStreamColumns:_*)

      //anonymize complete columns
      val completeAnonymizationColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and anonymization_flag='complete'").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val completeAnonymizeColumnsList = identifyColumns(inputStreamColumns, completeAnonymizationColumns).distinct
      val completeHashedDf = hashItComplete(hashJoinColDf, NUMBITS256, completeAnonymizeColumnsList: _*)

      //anonymize partial columns
      val partialAnonymizationColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and anonymization_flag='partial'").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val partialAnonymizeColumnsList = identifyColumns(inputStreamColumns, partialAnonymizationColumns).distinct
      val partialHashedDf = hashItPartial(completeHashedDf, partialAnonymizeColumnsList: _*)

      //encrypt columns
      val encryptionColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and encryption_flag=true").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val encryptionColumnsList = identifyColumns(inputStreamColumns, encryptionColumns).distinct
      val encryptedDf = encryptIt(partialHashedDf, encryptionColumnsList: _*)

      val transformedDf = hashNumericalIt(encryptedDf,"grouping_jk",joinKeys:_*)
        .withColumn("insert_at",lit(current_timestamp()))
        .withColumn("deleted_flag",lit(false))

      val firstOrderColumns = joinKeys.map(_.toString()) ++= ArrayBuffer("grouping_jk","insert_at","deleted_flag")
      val dfColumns = transformedDf.columns.toBuffer
      val lastOrdeColumns = dfColumns diff firstOrderColumns

      val finalTransformedDf =  transformedDf.selectExpr((firstOrderColumns ++= lastOrdeColumns):_*)



      logger.info(s"omittedColumnsList is ${omittedColumnsList.mkString("Array(", ", ", ")")}")
      logger.info(s"completeAnonymizeColumnsList is ${completeAnonymizeColumnsList.mkString("Array(", ", ", ")")}")
      logger.info(s"partialAnonymizationColumns is ${partialAnonymizationColumns.mkString("Array(", ", ", ")")}")
      logger.info(s"encryptionColumnsList is ${encryptionColumnsList.mkString("Array(", ", ", ")")}")
      logger.info(s"joinKeys is ${joinKeys.mkString("Array(", ", ", ")")}")

      returnMap += PROCESSEDDF -> finalTransformedDf
      returnMap += RAWDF -> rawDf
      taskOutputDataFrames = returnMap
      println("INFO: Processer ended")
      updateAndStoreStatus(PipelineNodeStatus.Finished)
      returnMap
    }
    catch {
      case e :Exception =>{
        println(e)
        updateAndStoreStatus(PipelineNodeStatus.Error,e)
        println("got exception")
      }
    }
    returnMap

  }
}

class ProcessorDeltaStreamSCD2() extends BaseProcessor {
  override def call(): Any = {
    val returnMap = new mutable.HashMap[String, DataFrame]()
    try {

      updateAndStoreStatus(PipelineNodeStatus.Started)
      println("INFO: Processor started")
      val inputStream = this.inputDataframe(PROCESSEDDF)
      val rawDf = this.inputDataframe(RAWDF)
      val productName = this.productName
      val joinKeys = extractJoinKeys(this.mergeSCD2Options.joinKeys)

      val updatesDF = inputStream

      val piiDf = spark.read.table(s"$ORCHESTRATION_DB.$PII_COLUMN_DETAILS_TABLE_NAME").filter(s"product_name ='$productName'")

      //Omit columns
      val omittedColumns = spark.read.table(s"$ORCHESTRATION_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME").filter(s"product_name ='${productName}' and table_name='${this.tableName}'").selectExpr("coalesce(omitted_cols,'')").collect()(0)(0).toString
      val omittedColumnsList = omittedColumns.split(",").distinct
      val omittedDf = dropIt(updatesDF, omittedColumnsList: _*)
      val inputStreamColumns = omittedDf.columns


      //anonymize complete columns
      val completeAnonymizationColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and anonymization_flag='complete'").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val completeAnonymizeColumnsList = identifyColumns(inputStreamColumns, completeAnonymizationColumns).distinct
      val completeHashedDf = hashItComplete(omittedDf, NUMBITS256, completeAnonymizeColumnsList: _*)

      //anonymize partial columns
      val partialAnonymizationColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and anonymization_flag='partial'").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val partialAnonymizeColumnsList = identifyColumns(inputStreamColumns, partialAnonymizationColumns).distinct
      val partialHashedDf = hashItPartial(completeHashedDf, partialAnonymizeColumnsList: _*)

      //encrypt columns
      val encryptionColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and encryption_flag=true").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val encryptionColumnsList = identifyColumns(inputStreamColumns, encryptionColumns).distinct
      val encrypteddDf = encryptIt(partialHashedDf, encryptionColumnsList: _*)

      val transformedDf = joinKeys.foldLeft(encrypteddDf) {
        case (tmpdf, columnName) => tmpdf.withColumn(columnName.toString(), coalesce(columnName,lit("n/a")))
      }

      val transformedDf_1 = hashNumericalIt(transformedDf,"grouping_jk",joinKeys:_*)
        .withColumn("current_flag",lit(true).cast(BooleanType))
        .withColumn("expiry_at",lit(null).cast(LongType))
        .withColumn("deleted_flag",lit(false))

      val firstOrderColumns = joinKeys.map(_.toString()) ++= ArrayBuffer("grouping_jk","current_flag","expiry_at","deleted_flag")
      val dfColumns = transformedDf_1.columns.toBuffer
      val lastOrdeColumns = dfColumns diff firstOrderColumns

      val finalTransformedDf =  transformedDf_1.selectExpr((firstOrderColumns ++= lastOrdeColumns):_*)



      returnMap += PROCESSEDDF -> finalTransformedDf
      returnMap += RAWDF -> rawDf
      taskOutputDataFrames = returnMap
      println("INFO: Processer ended")
      updateAndStoreStatus(PipelineNodeStatus.Finished)

    }
    catch {
      case e: Exception => {
        println(e)
        updateAndStoreStatus(PipelineNodeStatus.Error, e)
        println("got exception")
      }
    }

  }
}


class ProcessorDeltaStreamSCD4() extends BaseProcessor {
  override def call(): Any = {
    val returnMap = new mutable.HashMap[String, DataFrame]()
    try {

      updateAndStoreStatus(PipelineNodeStatus.Started)
      println("INFO: Processor started")
      val inputStream = this.inputDataframe(PROCESSEDDF)
      val rawDf = this.inputDataframe(RAWDF)
      val productName = this.productName
      val joinKeys = extractJoinKeys(this.mergeSCD4Options.joinKeys)

      val updatesDF = inputStream

      val piiDf = spark.read.table(s"$ORCHESTRATION_DB.$PII_COLUMN_DETAILS_TABLE_NAME").filter(s"product_name ='$productName'")

      //Omit columns
      val omittedColumns = spark.read.table(s"$ORCHESTRATION_DB.$TABLE_TOPIC_DETAILS_TABLE_NAME").filter(s"product_name ='${productName}' and table_name='${this.tableName}'").selectExpr("coalesce(omitted_cols,'')").collect()(0)(0).toString
      val omittedColumnsList = omittedColumns.split(",").distinct
      val omittedDf = dropIt(updatesDF, omittedColumnsList: _*)
      val inputStreamColumns = omittedDf.columns


      //anonymize complete columns
      val completeAnonymizationColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and anonymization_flag='complete'").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val completeAnonymizeColumnsList = identifyColumns(inputStreamColumns, completeAnonymizationColumns).distinct
      val completeHashedDf = hashItComplete(omittedDf, NUMBITS256, completeAnonymizeColumnsList: _*)

      //anonymize partial columns
      val partialAnonymizationColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and anonymization_flag='partial'").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val partialAnonymizeColumnsList = identifyColumns(inputStreamColumns, partialAnonymizationColumns).distinct
      val partialHashedDf = hashItPartial(completeHashedDf, partialAnonymizeColumnsList: _*)

      //encrypt columns
      val encryptionColumns: Array[(String, Boolean)] = piiDf.filter(s"product_name = '${productName}' and encryption_flag=true").select("pii_column_name", "common_flag").collect.map(row => (row.getString(0), row.getBoolean(1)))
      val encryptionColumnsList = identifyColumns(inputStreamColumns, encryptionColumns).distinct
      val encrypteddDf = encryptIt(partialHashedDf, encryptionColumnsList: _*)


      val transformedDf_1 = hashNumericalIt(encrypteddDf,"grouping_jk",joinKeys:_*)
                            .withColumn("deleted_flag",lit(false))
      val firstOrderColumns = joinKeys.map(_.toString()) ++= ArrayBuffer("grouping_jk","deleted_flag")
      val dfColumns = transformedDf_1.columns.toBuffer
      val lastOrdeColumns = dfColumns diff firstOrderColumns

      val finalTransformedDf =  transformedDf_1.selectExpr((firstOrderColumns ++= lastOrdeColumns):_*)

      returnMap += PROCESSEDDF -> finalTransformedDf
      returnMap += RAWDF -> rawDf
      taskOutputDataFrames = returnMap
      println("INFO: Processer ended")
      updateAndStoreStatus(PipelineNodeStatus.Finished)

    }
    catch {
      case e: Exception => {
        println(e)
        updateAndStoreStatus(PipelineNodeStatus.Error, e)
        println("got exception")
      }
    }

  }
}

