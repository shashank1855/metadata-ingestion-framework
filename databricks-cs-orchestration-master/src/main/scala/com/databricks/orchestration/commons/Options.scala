package com.databricks.genericPipelineOrchestrator.commons

import scala.collection.mutable.ListBuffer

case class Option(key:String, value:Object)

class Options() {
  var options = new ListBuffer[Option]()
  def addOption(option :Option): Unit = options += option;

  def get(key :String): Option =
  {
    return null;
  }

  def getValue(key :String): String =
  {
    return null;
  }

}
