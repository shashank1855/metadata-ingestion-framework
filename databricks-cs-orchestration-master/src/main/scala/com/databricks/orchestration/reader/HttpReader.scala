package com.databricks.genericPipelineOrchestrator.reader

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode, translate}

object HttpReader {
  var path=""
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")
    if(args.isEmpty) {
      val message = s"Please pass path to write csv file"
      throw new Exception(message)
    }
    path=args(0)
    val api_key=dbutils.secrets.get(test_DATABRICKS_SCOPE, HTTP_API_KEY)
    val url= HTTP_BASE_URL + api_key + HTTP_FILTERS
    processHttp(url,spark)
  }

  def processHttp(url:String,spark:SparkSession):Unit={

    val result2 = List(scala.io.Source.fromURL(url).mkString)
    val httpRDD=spark.sparkContext.makeRDD(result2)
    val httpDF=spark.read.json(httpRDD).coalesce(1)
    val linkDf =httpDF.select("links")
    val attributesDF = httpDF.withColumn("explodedData",explode(col("data"))).select("explodedData")
    val baseDF = attributesDF.select("explodedData.id",
      "explodedData.attributes.email",
      "explodedData.attributes.created_at",
      "explodedData.attributes.updated_at",
      "explodedData.attributes.custom_avatar_url",
      "explodedData.attributes.unsubscribed",
      "explodedData.attributes.registration_status")
    val returnDF = baseDF
      .withColumn("updated_at", translate(col("updated_at"),"T"," "))
      .withColumn("updated_at",translate(col("updated_at"),"Z",""))
      .withColumn("created_at", translate(col("created_at"),"T"," "))
      .withColumn("created_at",translate(col("created_at"),"Z",""))
      .withColumnRenamed("id","people_id")
      .withColumnRenamed("custom_avatar_url","avatar_url")
      .withColumnRenamed("unsubscribed","subscription")
      .withColumnRenamed("registration_status","status")
    returnDF.show(false)
    linkDf.printSchema()
    if(linkDf.select("links.*").columns.contains("next")) {
      val nextData = linkDf.select("links.next").collect().head.get(0).toString
      println(nextData)
      val api_key= dbutils.secrets.get(test_DATABRICKS_SCOPE, HTTP_API_KEY)
      val key = "?api_key=" + api_key + "&"
      val next_url = nextData.replace("?", key)
      println("NEXT URL : " + next_url)
        returnDF.write.mode("append").csv(path)
        processHttp(next_url,  spark)
      }
    returnDF.write.mode("append").csv(path)
    }


}
