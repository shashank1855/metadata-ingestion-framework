package com.databricks.genericPipelineOrchestrator.utility

import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant.{ORCHESTRATION_DB, TABLE_TOPIC_DETAILS_TABLE_NAME}
import com.databricks.genericPipelineOrchestrator.utility.Utility._
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.types._

import java.io.File
import scala.reflect.io.Directory

class UtilityTest extends AnyFunSuite with BeforeAndAfterAll {
   var spark: SparkSession = null
  @transient var deltaDir: Directory = null



  override protected  def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    deltaDir = Directory(new File("/tmp/delta/test"))
    deltaDir.deleteRecursively()

    spark.sql(s"CREATE DATABASE $ORCHESTRATION_DB LOCATION '/tmp/delta/test/$ORCHESTRATION_DB'")
    spark.sql(
      s"""
         |create table $ORCHESTRATION_DB.pii_datatable
         |(
         |id int,
         |description string
         |) using delta
         |""".stripMargin)
    val pii_data_anony_df = spark.read.option("wholeFile", true)
      .option("multiline",true)
      .option("header", true)
      .option("inferSchema", "true")
      .csv("src/test/resources/pii_data_anony.csv")

    pii_data_anony_df.write.mode("append").format("delta").saveAsTable(s"$ORCHESTRATION_DB.pii_datatable")
  }

  override protected  def afterAll(): Unit = {
    deltaDir.deleteRecursively()
    spark.stop()
  }

  test("1"){

    val df = spark.read.table(s"$ORCHESTRATION_DB.pii_datatable")
    df.show(false)
    println("before change")
    df.withColumn("description",anonymizePIIDataUDF(col("description"))).show(false)
    println("after change")
  }

  test("2"){
    println(buildInnerJoinCondition(Seq("id = idd","Name = ename") :+ "grouping_jk",None))
    val s = spark.sparkContext.parallelize(Seq(1,2,3)).map(r => Row(r))
    val df = spark.createDataFrame(s,StructType(Seq(StructField("id", IntegerType,true))))
    println(buildDPPForPartitionKeys(df,Seq("id")))


    var innerJoinConditionsFinal = buildInnerJoinCondition(Seq("id = idd","Name = ename") :+ "grouping_jk",None)
    println(innerJoinConditionsFinal)

    //add DPP for better merge
    innerJoinConditionsFinal = Option(Seq("id","id")) match {
      case Some(value) =>innerJoinConditionsFinal && col(buildDPPForPartitionKeys(df,value).replaceAll("and",""))
      case None => innerJoinConditionsFinal
    }

    println(innerJoinConditionsFinal)


    //generate condition from join keys
    var conditions = if(hasColumn(df,"grouping_jk")){buildInnerJoinCondition(Seq("id") :+ "grouping_jk" , None).toString()} else {buildInnerJoinCondition(Seq("id"), None).toString()}
    println(s"conditions :  $conditions")

    //add DPP for better merge
    conditions = Option(Seq("id","id")) match {
      case Some(value) => conditions + buildDPPForPartitionKeys(df,value)
      case None => conditions
    }

    println(conditions)


  }

}
