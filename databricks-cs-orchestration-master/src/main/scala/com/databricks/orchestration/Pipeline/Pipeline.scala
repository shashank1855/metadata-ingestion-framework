package com.databricks.genericPipelineOrchestrator.Pipeline


import com.databricks.genericPipelineOrchestrator.commons.{OrchestrationConstant, Task, TaskOutputType}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.concurrent.{Callable, Executors}
import java.util.{Calendar, Date}
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}


object PipelineNodeStatus extends Enumeration {
  val None, init, Started, Finished, Error, dead_end, Running = Value
}

class  PipelineGraph {
  val logger = Logger.getLogger(getClass)
  var nodes: Map[String, PipelineNode] = Map()
  var edges: Map[String, PipelineEdge] = Map()
  var status = "Active"
  var rerunPipeline:Boolean=false
  var pipeLineUid: String= null
  var pipeLineName:String = null

  def add_node(node: PipelineNode): PipelineNode = {
    if (nodes.contains(node.key)) {
      return nodes(node.key)
    } else {
      nodes += (node.key -> node)
      return node;
    }

  }


  def all_nodes_done: Boolean = {
    var status:Boolean=true
    for (v <- nodes.values) {
      if(v.status == PipelineNodeStatus.Error){
        logger.info("Got error , Shutting down the system "+v.getTask().taskName)
         return true
      }
      if ((v.status != PipelineNodeStatus.Finished) ) {
        status = false
      }
      /*if ((v.status != PipelineNodeStatus.Finished) ) {
        return false
      }*/

    }
    return status
  }


  def add_node(node1: PipelineNode, node2: PipelineNode): Unit = {
    val edge_id = java.util.UUID.randomUUID.toString;
    val edge = PipelineEdge(edge_id, node1, node2)
    node1.out_edges += edge
    node2.in_edges += edge
    edges += (edge.edge_id -> edge);
  }


  def add_node(sample: String): PipelineNode = {
    if (!nodes.contains(sample))
      return null;
    return nodes(sample)
  }


  def get_node(sample: String): PipelineNode = {
    if (!nodes.contains(sample))
      return null;
    return nodes(sample)
  }


  def get_edge(edge_id: String): PipelineEdge = return edges(edge_id)

  def get_level1_node(status: String): ListBuffer[PipelineNode] = {
    var tmpnodes = ListBuffer[PipelineNode]()
    for (v <- nodes.values) {
      if (v.in_edges.length < 1)
        tmpnodes += v

    }
    return tmpnodes

  }


  def get_previous_done_nodes(node: PipelineNode):  ListBuffer[PipelineNode] = {
    var tmpnodes = ListBuffer[PipelineNode]()
    val in_edges = node.in_edges
    if (in_edges.length < 1)
      return tmpnodes;

    for (e <- in_edges) {
      tmpnodes += e.from_node

    }
    (tmpnodes)
  }

  def get_nodes_for_execution: ListBuffer[PipelineNode] = {
    var tmpnodes = ListBuffer[PipelineNode]()
    for(v<-nodes.values){
      updateNodeStatus(v,v.getTask().taskStatus)
    }
    for (v <- nodes.values) {

      if (v.status == PipelineNodeStatus.None) {
        if (v.in_edges.length < 1) {
          tmpnodes += v
        }
        else if (all_previous_done_nodes(v) == true) {
          tmpnodes += v
        }
      }

    }
    return tmpnodes
  }

  def marked_finished(job_node: PipelineNode): Unit = {
    job_node.end_time = Calendar.getInstance().getTime()
    updateNodeStatus(job_node, PipelineNodeStatus.Finished)
  }

  def marked_error(job_node: PipelineNode): Unit = {
    job_node.end_time = Calendar.getInstance().getTime()
    updateNodeStatus(job_node, PipelineNodeStatus.Finished)
  }

  def marked_started(job_node: PipelineNode): Unit = {
    job_node.start_time = Calendar.getInstance().getTime()
    updateNodeStatus(job_node, PipelineNodeStatus.Finished)
  }


  def updateNodeStatus(job_node: PipelineNode, nodeStatus: PipelineNodeStatus.Value): Unit = {
    job_node.status = nodeStatus;
  }

  def all_previous_done_nodes(job_node: PipelineNode): Boolean = {
    val in_edges = job_node.in_edges
    if (in_edges.length < 1)
      return false;

    for (e <- in_edges) {
      if (e.from_node.status == PipelineNodeStatus.None || e.from_node.status == PipelineNodeStatus.Started)
        return false

      if (e.from_node.status == PipelineNodeStatus.dead_end) {
        job_node.status = PipelineNodeStatus.dead_end
        return false
      }
    }
    return true;
  }

  def add_Edge(node1: PipelineNode, node2: PipelineNode): Unit = {
    val edge_id = java.util.UUID.randomUUID.toString;
    val edge = PipelineEdge(edge_id, node1, node2)
    node1.out_edges += edge
    node2.in_edges += edge
    edges += (edge.edge_id -> edge);
  }

}


class PipelineNode(name: String, task: Task) {
  var out_edges = new ListBuffer[PipelineEdge]
  var in_edges = new ListBuffer[PipelineEdge]
  var status = PipelineNodeStatus.None
  var start_time: Date = null
  var end_time: Date = null
  var key = name;

  def getTask(): Task = task

  def setStatus(newstatus: PipelineNodeStatus.Value): Unit = {
    this.status = newstatus
  }

}

case class PipelineEdge(edge_id: String, from_node: PipelineNode, to_node: PipelineNode)

class Pipeline(pipelineGraph: PipelineGraph) extends Callable[String]{
  val logger = Logger.getLogger(getClass)
  var sparkSession: SparkSession = null;

  def getPipelineGraph():PipelineGraph={
  pipelineGraph
  }

  def addSparkSessio(sparkSession: SparkSession) = this.sparkSession = sparkSession
  def start(): Unit = {
    logger.info("Pipeline started")
    var nodes = pipelineGraph.get_nodes_for_execution
    while (pipelineGraph.all_nodes_done == false) {
      val executorService = Executors.newFixedThreadPool(nodes.size)
      import java.util
      var todo = new util.ArrayList[Task](nodes.size)
      for (n <- nodes) {
        val task = n.getTask()
        logger.info("Task in hand " + n.key)
        task.spark=this.sparkSession
        task.taskName=n.key
        val previousNode:  ListBuffer[PipelineNode] = pipelineGraph.get_previous_done_nodes(n)
        var inputResultMap=new mutable.HashMap[String,DataFrame]()
        if (previousNode.length > 0 )
        {
          previousNode.foreach(node =>{
            if(node.getTask().taskOutputDataFrames != null)
           inputResultMap = inputResultMap.++(node.getTask().taskOutputDataFrames)
          })
          task.inputDataframe = inputResultMap
        }

        todo.add(task)
      }

      executorService.invokeAll(todo)
      nodes = pipelineGraph.get_nodes_for_execution
    }

  }

  def stop(): Unit = {
    // star the pipeline
  }

  override def call(): String = {
    start()
    ""
  }
}
