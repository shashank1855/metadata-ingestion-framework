package com.databricks.genericPipelineOrchestrator.utility.udtest

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.genericPipelineOrchestrator.utility.Crypt.{encrypt, saltkey, scope, secretekey}
import org.apache.hadoop.hive.ql.exec.UDF

class Encrypt extends UDF{
  def evaluate(input: String): String = {
    encrypt(dbutils.secrets.get(scope,secretekey),dbutils.secrets.get(scope,saltkey), input)
  }
}
