package com.databricks.genericPipelineOrchestrator.reader
import com.databricks.genericPipelineOrchestrator.commons.{Options, Task}

abstract class Basereader  extends  Task{
  var options:Options = null
}
