package com.databricks.genericPipelineOrchestrator.utility.udtest

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.genericPipelineOrchestrator.utility.Crypt.{decrypt, saltkey, scope, secretekey}
import org.apache.hadoop.hive.ql.exec.UDF

class Decrypt extends UDF{
  def evaluate(input: String): String = {
    decrypt(dbutils.secrets.get(scope,secretekey),dbutils.secrets.get(scope,saltkey), input)
  }
}
