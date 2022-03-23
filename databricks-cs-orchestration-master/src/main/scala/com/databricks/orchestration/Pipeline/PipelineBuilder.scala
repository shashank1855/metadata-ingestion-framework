package com.databricks.genericPipelineOrchestrator.Pipeline

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant.{MYSQL_DRIVER, URL_STRING, URL_TRAIL_STRING}
import com.databricks.genericPipelineOrchestrator.commons.{OrchestrationConstant, PipeLineType, Task}
import com.databricks.genericPipelineOrchestrator.writter.{mergeSCD1Options, mergeSCD2Options, mergeSCD4Options}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

class PipelineBuilder {


  private var pipelineGraph: PipelineGraph = new PipelineGraph()
  private var pipeline: Pipeline = new Pipeline(pipelineGraph)
  private var pName: String = ""
  private var pipelineId: String = ""
  private var runId: String = ""
  private var productName: String = ""
  private var topicName: String = ""
  private var batchId: String = ""
  private var mergeSCD1Options: mergeSCD1Options = null
  private var mergeSCD2Options: mergeSCD2Options = null
  private var mergeSCD4Options: mergeSCD4Options = null
  private var tableName: String = ""
  private var partitionSize: Int = 0
  private var pipelineDefId: String = ""
  private var pipelineType: PipeLineType.Value = PipeLineType.CDC

  def addSparkSession(sparkSession: SparkSession): PipelineBuilder = {
    pipeline.sparkSession = sparkSession
    (this)
  }

  def setBatchId(batchId: String): PipelineBuilder = {
    this.batchId = batchId
    this
  }

  def setPipelineType(pipelineType: PipeLineType.Value): PipelineBuilder = {
    this.pipelineType = pipelineType
    this
  }

  def setProductName(productName: String): PipelineBuilder = {
    this.productName = productName
    this
  }

  def setTopicName(topicName: String): PipelineBuilder = {
    this.topicName = topicName
    this
  }

  def setPipelineDefId(pipelineDefId: String): PipelineBuilder = {
    this.pipelineDefId = pipelineDefId
    this
  }

  def setPartitionSize(partitionSize: Int): PipelineBuilder = {
    this.partitionSize = partitionSize
    this
  }

  def setMergeSCD1Options(mergeSCD1Options: mergeSCD1Options): PipelineBuilder = {
    this.mergeSCD1Options = mergeSCD1Options
    this
  }

  def setMergeSCD2Options(mergeSCD2Options: mergeSCD2Options): PipelineBuilder = {
    this.mergeSCD2Options = mergeSCD2Options
    this
  }

  def setMergeSCD4Options(mergeSCD4Options: mergeSCD4Options): PipelineBuilder = {
    this.mergeSCD4Options = mergeSCD4Options
    this
  }

  def setTableName(tableName: String): PipelineBuilder = {
    this.tableName = tableName
    this
  }

  def setBasicInfo(task: Task): Task = {
    task.pipelineName = pName
    task.pipeLineUid = pipelineId
    task.taskStatus = PipelineNodeStatus.None
    task.runId = runId
    task.productName = productName
    task.topicName = topicName
    task.mergeSCD1Options = mergeSCD1Options
    task.mergeSCD2Options = mergeSCD2Options
    task.mergeSCD4Options = mergeSCD4Options
    task.batchId = batchId
    task.logger = Logger.getLogger(task.getClass)
    task.tableName = tableName
    task.partitionSize = partitionSize
    task.pipelineDefId = pipelineDefId
    if (pipelineType == PipeLineType.JDBC) {
      populateJDBCReaderOptions(task)
    }
    task
  }

  private def populateJDBCReaderOptions(task: Task): Task = {

    val readerOptionsMap = new mutable.HashMap[String, String]()

    pipeline.sparkSession.sql("select * from global_temp.shard_details").collect().foreach(r => {
      val scope = r.getAs("secret_scope").toString
      val secretUser = r.getAs("secret_user").toString
      val secretPassword = r.getAs("secret_password").toString
      val user = dbutils.secrets.get(scope, secretUser)
      val password = dbutils.secrets.get(scope, secretPassword)
      val databaseName = r.getAs("database_name").toString
      val endPoint = r.getAs("endpoint").toString
      val url = URL_STRING + endPoint + "/" + databaseName + URL_TRAIL_STRING

      readerOptionsMap.put("driver", MYSQL_DRIVER)
      readerOptionsMap.put("url", url)
      readerOptionsMap.put("user", user)
      readerOptionsMap.put("password", password)
    })

    task.readerOptions = readerOptionsMap
    task
  }

  def addTask(key: String, task: Task): PipelineBuilder = {
    setBasicInfo(task)
    val pipelineNode = createPipelineNode(key, task)
    pipelineGraph.add_node(pipelineNode)
    (this)
  }


  def addAfter(afterNodeKey: String, key: String, task: Task): PipelineBuilder = {
    setBasicInfo(task)
    val pipelineNode = createPipelineNode(key, task)
    pipelineGraph.add_node(pipelineNode)
    val previousNode = pipelineGraph.get_node(afterNodeKey)
    pipelineGraph.add_Edge(previousNode, pipelineNode)
    return (this)
  }

  def createPipelineNode(key: String, task: Task): PipelineNode = {
    if (pipelineGraph.nodes.contains(key)) {
      return pipelineGraph.get_node(key)
    }
    setBasicInfo(task)
    var pipelineNode: PipelineNode = new PipelineNode(key, task)
    (pipelineNode)
  }

  def build(): Pipeline = {
    // all generic validation can go here
    (pipeline)
  }

  def setPipelineName(name: String): PipelineBuilder = {
    this.pName = name;
    pipelineGraph.pipeLineName = name

    def uuid = java.util.UUID.randomUUID.toString

    this.pipelineId = uuid
    this
  }

  def setRunId(id: String): PipelineBuilder = {
    this.runId = id
    this
  }


}


object PipelineBuilder {
  def start(): PipelineBuilder = {
    return new PipelineBuilder()
  }
}
