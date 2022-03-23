package com.databricks.genericPipelineOrchestrator.commons

object OrchestrationConstant {

  val SQLITE_URL = "jdbc:sqlite:/Users/sriram.mohanty/sqlite3-test/test.db"
 val AVRO_SCHEMA_REG_URL = "http://test.in:8081/"

  val MATCH = "match"
  val NOTMATCH = "notmatch"
  val DELETE ="delete"
 val READER = "_reader"
 val WRITER = "_writer"
 val PROCESSOR = "_processer"
  val CDC_READER = "_cdc_reader"
  val CDC_WRITER = "_cdc_writer"
  val CDC_PROCESSOR = "_cdc_processer"
  val HISTORY_READER = "_history_reader"
  val HISTORY_WRITER = "_history_writer"
  val HISTORY_PROCESSOR = "_history_processer"
  val JDBC_READER = "_jdbc_reader"
  val JDBC_WRITER = "_jdbc_writer"
  val JDBC_PROCESSOR = "_jdbc_processor"
  val HISTORY_TABLE_TAG = "_history"


  val NUMBITS224 = 224
  val NUMBITS256 = 256
  val NUMBITS384 = 384
  val NUMBITS512 = 512
  val NUMBITS0 = 0 //equivalent to 256

  val ORCHESTRATION_DB = "orchestration_db"
  val PII_COLUMN_DETAILS_TABLE_NAME = "pii_column_details"
  val TABLE_TOPIC_DETAILS_TABLE_NAME = "table_details"
  val SHARD_DETAILS_TABLE_NAME = "shard_details"
  val INTERNAL_SHARD_MAPPINGS_TABLE_NAME = "internal_shard_mappings"
  val PIPELINE_BATCH_MAP = "pipeline_batch_map"
  val FACT_TABLE_NAME = "pipeline_fact"
  val STATUS_TABLE_NAME = "pipeline_status"
  val ERROR_TABLE_NAME = "pipeline_error_logs"
  val RAWDF = "rawdf"
  val PROCESSEDDF = "processedDf"
  val READER_TYPE="reader_type"
  val READER_OPTIONS="reader_options"
  val STARTING_OFFSET="latest"
  val KAFKA_BOOTSTRAP_SERVER = "cdc01.test.in:9092,cdc02.test.in:9092,cdc03.test.in:9092"
  val HISTORYLOADCHECKPOINTLOC = "dbfs:/mnt/data-lake-test-bronze-data-store-us-east-1/bronze/test_cdc/checkpointlocation"

 val HTTP_BASE_URL="https://api.test.com/v2/people?api_key="
 val test_DATABRICKS_SCOPE="test-databricks-scope"
 val HTTP_API_KEY="http-api-key"
 val HTTP_FILTERS="&limit=100"

  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
  val URL_STRING = "jdbc:mysql://"
  val URL_TRAIL_STRING = "?tinyInt1isBit=false&zeroDateTimeBehavior=round&connectTimeout=0&socketTimeout=0&autoReconnect=true&failOverReadOnly=false&tcpKeepAlive=true"

 val ccPattern = "\\b3[47]\\d{2}[-]?\\d{6}[-]?\\d{5}\\b|\\b4\\d{3}[-]?\\d{4}[-]?\\d{4}[-]?\\d{4}\\b|\\b5[1-5]\\d{2}[-]?\\d{4}[-]?\\d{4}[-]?\\d{4}\\b|\\b(30[0-5]\\d|3[68]\\d{2})[-]?\\d{4}[-]?\\d{4}[-]?\\d{2}\\b|\\b[xX]{4}[-]?[xX]{4}[-]?[xX]{4}[-]?\\d{4}\\b|\\b\\d{4}[-]?[xX]{4}[-]?[xX]{4}[-]?[xX]{4}\\b"
 //val ccPatternFlag = "credit[\\s]?card"
 val ssnPattern = "\\b(?!000|666)[0-8][0-9]{2}-(?!00)[0-9]{2}-(?!0000)[0-9]{4}\\b"
 val emailPattern = "\\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}\\b"
 //val agePatternFlag = "\\bage\\s?(is|:|-|of)?\\b"
 //val namePatternFlag = "\\bname\\s?(is|:|-)?\\b"
 //val ssnPatternFlag = "\\bssn\\s?(is|:|-)?\\b"

 val PII_DATA_PATTERN = Array(
  ccPattern
  //,ccPatternFlag
  ,ssnPattern
  ,emailPattern
  //,agePatternFlag
  //,namePatternFlag
  //,ssnPatternFlag
 )

 val HISTORYLOADMAXFILEMAP = Map("flexifields" -> 2)

}
object StatusColumns extends Enumeration {
 val topic, partition, startOffset, endOffset, pipelineName, instanceName, status,pipelineId,runId,errorMsg,stackTrace,lastUpdate,batchId,pipelineDefId,lastUpdateDate = Value
}

object FactColumns extends Enumeration {
 val runId, pipelineId, pipelineName, instanceName, topic, inputCount, outputCount,lastUpdate,pipelineDefId,lastUpdateDate = Value
}

object ErrorTableColumns extends Enumeration {
 val errorData, insertTime = Value
}

object PipeLineType extends Enumeration {
 val CDC, JDBC,HTTP = Value
}