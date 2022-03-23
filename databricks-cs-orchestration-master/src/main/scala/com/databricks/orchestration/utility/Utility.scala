package com.databricks.genericPipelineOrchestrator.utility

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.databricks.genericPipelineOrchestrator.Pipeline.PipelineNodeStatus
import com.databricks.genericPipelineOrchestrator.commons.OrchestrationConstant.{DELETE, HISTORY_TABLE_TAG, MATCH, NOTMATCH, ORCHESTRATION_DB, PII_DATA_PATTERN, RAWDF, STARTING_OFFSET, STATUS_TABLE_NAME}
import com.databricks.genericPipelineOrchestrator.commons.{StatusColumns, Task}
import com.databricks.genericPipelineOrchestrator.utility.Crypt.{decrypt, encrypt, saltkey, scope, secretekey}
import com.databricks.genericPipelineOrchestrator.utility.Utility.writeSCD1
import com.databricks.genericPipelineOrchestrator.writter.{MergeBuilderLogicSCD1, MergeBuilderLogicSCD2}
import io.delta.tables.DeltaTable
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriter, Row, SparkSession, functions}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

import java.util.regex.Pattern
import java.util.Date
import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

object Utility {

  private val encryptUDF: UserDefinedFunction = udf(encrypt _)
  private val decryptUDF: UserDefinedFunction = udf(decrypt _)
  val anonymizePIIDataUDF: UserDefinedFunction = udf(anonymizePIIData _)
  val epochToTimestampUDF: UserDefinedFunction = udf(epochToTimestamp _)
  private val logger = Logger.getLogger(getClass)

  /**
   *
   * @param df      Input datarame
   * @param alias   alias column name after hashing column(s)
   * @param numBits The numBits indicates the desired bit length of the result, which must have a value of 224, 256, 384, 512, or 0 (which is equivalent to 256)
   * @param cols    Names of the column/list to be hashed
   * @return returns the dataframe with new hashed column
   */

  def hashIt(df: DataFrame, alias: String, numBits: Int, cols: String*): DataFrame = {
    val concatString = cols.foldLeft(lit(""))((x, y) => concat(x, coalesce(col(y).cast("string"), lit("n/a"))))
    df.withColumn(alias, sha2(concatString, numBits))
  }

  def hashItComplete(df: DataFrame, numBits: Int, cols: String*): DataFrame = {
    val newDf = cols.foldLeft(df) {
      case (tmpdf, columnName) => tmpdf.withColumn(s"${columnName}_hash", sha2(coalesce(col(columnName).cast("string"), lit("n/a")), numBits))
    }
    newDf
  }

  def hashItPartial(df: DataFrame, cols: String*): DataFrame = {
    val newDf = cols.foldLeft(df) {
      case (tmpdf, columnName) => tmpdf.withColumn(s"${columnName}_hash", anonymizePIIDataUDF(coalesce(col(columnName).cast("string"), lit("n/a"))))
    }
    newDf
  }

  def hashNumericalIt(df: DataFrame, alias: String, cols: Column*): DataFrame = {
    val concatString = cols.foldLeft(lit(""))((x, y) => concat(x, coalesce(y.cast("string"), lit("n/a"))))
    df.withColumn(alias, abs(hash(concatString)).mod(500))
  }

  /**
   *
   * @param df               input datafrem
   * @param pattern          pattern to identified and replaced. It accepts regular expression and normal pattern aswell. example : """(^[^@]{3}|(?!^)\G)[^@]"""
   * @param replaceVal       string to be replace old value. example: "$1*"
   * @param dropSourceColumn Boolean. true if you want to drop source column. false if you dont want to drop source column
   * @param cols             Names of the columns/list to be masked
   * @return returns the dataframe with new masked column
   */

  def maskIt(df: DataFrame, pattern: String, replaceVal: String, dropSourceColumn: Boolean, cols: String*): DataFrame = {
    if (dropSourceColumn) {
      val newDf = cols.foldLeft(df) {
        case (tmpdf, columnName) => tmpdf.withColumn(columnName, regexp_replace(col(columnName), pattern, replaceVal))
      }
      newDf
    }
    else {
      val newDf = cols.foldLeft(df) {
        case (tmpdf, columnName) => tmpdf.withColumn(s"${columnName}_masked", regexp_replace(col(columnName), pattern, replaceVal))
      }
      newDf
    }
  }

  /**
   * This function helps in encryptinh the data
   *
   * @param df   input dataframe
   * @param cols Names of the column/list to be encrypted
   * @return returns the dataframe with new encrypted column
   */


  def encryptIt(df: DataFrame, cols: String*): DataFrame = {
    val key = dbutils.secrets.get(scope, secretekey)
    val saltKey = dbutils.secrets.get(scope, saltkey)
    val df1 = df.withColumn("key", lit(key)).withColumn("saltkey", lit(saltKey))

    val newDf = cols.foldLeft(df1) {
      case (tmpdf, columnName) => tmpdf.withColumn(columnName, encryptUDF(col("key"), col("saltkey"), coalesce(col(columnName), lit("n/a")).cast("string")))
    }
    newDf.drop("key").drop("saltkey")

  }

  /**
   * This function helps in decrypting the data
   *
   * @param df   input dataframe
   * @param cols Names of the columns/list to be decrypted
   * @return returns the dataframe with new decrypted column
   */

  def decryptIt(df: DataFrame, cols: String*): DataFrame = {
    val key = dbutils.secrets.get(scope, secretekey)
    val saltKey = dbutils.secrets.get(scope, saltkey)
    val df1 = df.withColumn("key", lit(key)).withColumn("saltkey", lit(saltKey))


    val newDf = cols.foldLeft(df1) {
      case (tmpdf, columnName) => tmpdf.withColumn(columnName, decryptUDF(col("key"), col("saltkey"), coalesce(col(columnName), lit("n/a")).cast("string")))
    }
    val finalDf = cols.foldLeft(newDf) {
      case (tmpdf, columnName) => tmpdf.withColumn(columnName, when(col(columnName).equalTo("n/a"), null).otherwise(col(columnName)))
    }
    finalDf.drop("key").drop("saltkey")
  }

  /**
   *
   * @param df   input dataframe
   * @param cols Names of the column/list to be droped
   * @return returns the dataframe after droping the columns
   */

  def dropIt(df: DataFrame, cols: String*): DataFrame = {

    val newDf = cols.foldLeft(df) {
      case (tmpdf, columnName) => tmpdf.drop(columnName)
    }
    newDf

  }

  /**
   * This fucntion helps to check if column is present in dataframe or not
   *
   * @param df      input dataframe
   * @param colName column name which has to be checked
   * @return Boolean
   */

  def hasColumn(df: DataFrame, colName: String): Boolean = Try(df(colName)).isSuccess


  /**
   * This function helps in hashing the test
   *
   * @param text String which has to be hashed
   * @return hashed string
   */
  def sha256Hash(text: String): String = {
    String.format("%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8"))))
  }

  /**
   * This function helps in anonymizing the PII data in free text. For now it uses PII_DATA_PATTERN has constatnt for patterns.
   *
   * @param input Text which has to be hashed
   * @return hashed string
   */
  def anonymizePIIData(input: String): String = {

    def internalPIIAnony(regex: String, input: String): String = {
      var tmpSubstring = ""
      val p = Pattern.compile(regex)
      val m = p.matcher(input)
      val sb = new StringBuffer
      while ( {
        m.find
      }) {
        tmpSubstring = sha256Hash(m.group())
        m.appendReplacement(sb, tmpSubstring)
      }
      m.appendTail(sb)
      sb.toString
    }

    val outPutString = PII_DATA_PATTERN.foldLeft(input) {
      case (inputSring, regex) => internalPIIAnony(regex, inputSring)
    }

    outPutString
  }

  /**
   * This function helps in identifying if the column is present or not
   *
   * @param inputColumns   input set of columns out of which we need to identify
   * @param colsToIdentify columns which has to be identified. If Boolean is true then we do exact match. If not we do regex match
   * @return returns the Array of column names which are matching
   */
  def identifyColumns(inputColumns: Array[String], colsToIdentify: Array[(String, Boolean)]): Array[String] = {

    val columnsListBuffer = scala.collection.mutable.ArrayBuffer[String]()
    colsToIdentify.foldLeft(columnsListBuffer) {
      case (z, i) => if (i._2) {
        if (inputColumns.contains(i._1)) {
          z += i._1
        }
      } else {
        z ++= inputColumns.filter(_.contains(i._1)).toBuffer
      }
        z
    }
    columnsListBuffer.toArray
  }


  def getOffset(sparkSession: SparkSession, topicName: String, partitionSize: Int): String = {

    val sql = s"""select max(${StatusColumns.endOffset.toString}) as ${StatusColumns.endOffset.toString},${StatusColumns.topic.toString},${StatusColumns.partition.toString} from ${ORCHESTRATION_DB}.${STATUS_TABLE_NAME} where ${StatusColumns.topic.toString}='${topicName}' and ${StatusColumns.endOffset.toString} != 0 group by ${StatusColumns.topic.toString},${StatusColumns.partition.toString}"""
    val listOffset = sparkSession.sql(sql).collectAsList()
    if (listOffset.size() != partitionSize) {
      logger.info("Offset Information For Topic: " + topicName + ":" + STARTING_OFFSET)
      return STARTING_OFFSET
    }
    val partitionOffsetMap = new mutable.HashMap[String, Long]()
    listOffset.forEach(row => {
      val offset = row.getLong(0)
      val partition = row.getInt(2)
      partitionOffsetMap.put(partition.toString, offset)
      println(offset)

    })
    val startOffsetMap = new mutable.HashMap[String, mutable.Map[String, Long]]()
    startOffsetMap.put(topicName, partitionOffsetMap)
    import org.json4s.DefaultFormats
    import org.json4s.native.Json
    val result = Json(DefaultFormats).write(startOffsetMap)
    logger.info("Offset Information For Topic: " + topicName + ":" + result)
    result
  }

  /**
   * This Functions helps you generate join condition based on the keys provided for target and updates
   *
   * @param joinKeys           is a Seq[String] can be passed as Seq("id") or Seq("id","name = empName")
   * @param extraJoinCondition extra join condition
   * @return returns join condition build
   */
  def buildInnerJoinCondition(joinKeys: Seq[String], extraJoinCondition: Option[String]): Column = {
    val innerJoinConditions = joinKeys.map(cond => {
      val arr = cond.split("\\s+")
      if (arr.size == 3) {
        arr(1) match {
          case "<" => col(s"target.${arr(0)}") < col(s"updates.${arr(2)}")
          case "<=" => col(s"target.${arr(0)}") <= col(s"updates.${arr(2)}")
          case "=" => col(s"target.${arr(0)}") === col(s"updates.${arr(2)}")
          case ">=" => col(s"target.${arr(0)}") >= col(s"updates.${arr(2)}")
          case ">" => col(s"target.${arr(0)}") > col(s"updates.${arr(2)}")
          case "!=" => col(s"target.${arr(0)}") != col(s"updates.${arr(2)}")
        }
      }
      else if (arr.size == 1) {
        col(s"target.${arr(0)}") === col(s"updates.${arr(0)}")
      }
    })

    val innerJoinConditionsFinal = (extraJoinCondition match {
      case Some(value) => innerJoinConditions ++ List(expr(value))
      case None => innerJoinConditions
    }).asInstanceOf[List[Column]].reduce(_ && _)
    innerJoinConditionsFinal
  }

  def buildMergeJoinCondition(joinKeys: Seq[String], extraJoinCondition: Option[String]): (ListBuffer[String], ListBuffer[String], String) = {

    val targetTableCols: ListBuffer[String] = ListBuffer[String]()
    val updatesTableCols: ListBuffer[String] = ListBuffer[String]()
    joinKeys.foreach(cond => {
      val arr = cond.split("\\s+")
      if (arr.size == 3) {
        targetTableCols += arr(0)
        updatesTableCols += arr(2)
      }
      else if (arr.size == 1) {
        targetTableCols += arr(0)
        updatesTableCols += arr(0)
      }
    })

    val union1: ListBuffer[String] = ListBuffer[String]()
    val union2: ListBuffer[String] = ListBuffer[String]()
    var mergeJoinConditionList: ListBuffer[String] = ListBuffer[String]()
    for (i <- updatesTableCols.indices) {
      union1 += s"null as mergekey$i"
      union2 += s"${updatesTableCols(i)} as mergekey$i"
    }

    for (i <- targetTableCols.indices) {
      mergeJoinConditionList += s"target.${targetTableCols(i)} = updates.mergekey$i"
    }

    union1 += "*"
    union2 += "*"
    println(mergeJoinConditionList)

    mergeJoinConditionList = extraJoinCondition match {
      case Some(value) => mergeJoinConditionList ++ List(value)
      case None => mergeJoinConditionList
    }
    val mergeJoinCondition = mergeJoinConditionList.mkString(" and ")
    println(mergeJoinConditionList)
    (union1, union2, mergeJoinCondition)

  }

  def extractJoinKeys(joinKeys: Seq[String]): ListBuffer[Column] = {
    val updatesTableCols: ListBuffer[Column] = ListBuffer[Column]()
    joinKeys.foreach(cond => {
      val arr = cond.split("\\s+")
      if (arr.size == 3) {
        updatesTableCols += col(arr(2))
      }
      else if (arr.size == 1) {
        updatesTableCols += col(arr(0))
      }
    })
    updatesTableCols
  }

  def buildDPPForPartitionKeys(df: DataFrame, partitionKeys: Seq[String]): String = {

    var DPPString = ""
    partitionKeys.foreach(key => {
      val filter = (df.select(key).distinct().collect.map(row => row.getAs(key).toString)).mkString("'", "','", "'")
      DPPString = DPPString + s""" and target.$key in ($filter)"""
    })

    DPPString
  }

  def castEphocToTimestamp(df: DataFrame, colName: String): DataFrame = {
    if (hasColumn(df, colName)) {
      df.withColumn(colName, epochToTimestampUDF(col(colName)).cast("timestamp"))
    } else df
  }

  def epochToTimestamp(epochMillis: Long): String = {
    val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS+SSSS")
    df.format(epochMillis)
  }

  def checkTable(spark: SparkSession, tableName: String): Boolean = {
    val result = spark.catalog.tableExists(tableName)
    if (result) {
      logger.info(s"INFO: $tableName exists")
      println(s"INFO: $tableName exists")
    } else {
      logger.info(s"INFO: $tableName dosent exists")
      println(s"INFO: $tableName dosent exists")
    }
    result
  }

  /**
   *
   * Batch SCD starts
   *
   *
   *
   *
   *
   */

  /**
   *
   * @param queueDf
   * @param targetTableName
   * @param joinKeys
   * @param mergeBuilderLogic
   * @param extraJoinCondition
   * @param partitionKeys
   * @param dedupColumns
   */

  def writeSCD1(spark: SparkSession
                , queueDf: DataFrame
                , targetTableName: String
                , joinKeys: Seq[String]
                , mergeBuilderLogic: Option[Seq[MergeBuilderLogicSCD1]]
                , extraJoinCondition: Option[String]
                , partitionKeys: Option[Seq[String]] = None
                , dedupColumns: Option[Seq[String]] = None
                , task: Option[Task] = None): Unit = {
    try {
      //Removing the duplicates from stream df
      val joinKeysSelect = extractJoinKeys(joinKeys)
      val updatesDF = dedupColumns match {
        case Some(value) =>
          val window = Window.partitionBy(joinKeysSelect: _*).orderBy(value.head, value.tail: _*)
          queueDf.withColumn("rank", row_number().over(window))
            .filter("rank = 1")
            .drop("rank")
        case None => queueDf
      }

      if (checkTable(spark, targetTableName)) {
        //generate condition from join keys
        var conditions = if (hasColumn(updatesDF, "grouping_jk")) {
          buildInnerJoinCondition(joinKeys :+ "grouping_jk", extraJoinCondition).toString()
        } else {
          buildInnerJoinCondition(joinKeys, extraJoinCondition).toString()
        }

        //add DPP for better merge
        conditions = partitionKeys match {
          case Some(value) => conditions + buildDPPForPartitionKeys(updatesDF, value)
          case None => conditions
        }

        println(s"conditions :  $conditions")
        logger.info(s"conditions :  $conditions")

        //Build merge logic
        var deltaMergerBuilder = DeltaTable
          .forName(targetTableName)
          .as("target")
          .merge(updatesDF.as("updates"), s"$conditions")

        deltaMergerBuilder = mergeBuilderLogic match {
          case Some(value) => value.foldLeft(deltaMergerBuilder) {

            case (mergeBuilder, MergeBuilderLogicSCD1(MATCH, None, Some(condition), Some(value))) => mergeBuilder.whenMatched(condition).updateExpr(value)
            case (mergeBuilder, MergeBuilderLogicSCD1(MATCH, None, None, Some(value))) => mergeBuilder.whenMatched().updateExpr(value)
            case (mergeBuilder, MergeBuilderLogicSCD1(MATCH, None, Some(condition), None)) => mergeBuilder.whenMatched(condition).updateAll()
            case (mergeBuilder, MergeBuilderLogicSCD1(MATCH, None, None, None)) => mergeBuilder.whenMatched().updateAll()

            case (mergeBuilder, MergeBuilderLogicSCD1(MATCH, Some(DELETE), Some(condition), None)) => mergeBuilder.whenMatched(condition).delete()
            case (mergeBuilder, MergeBuilderLogicSCD1(MATCH, Some(DELETE), None, None)) => mergeBuilder.whenMatched().delete()

            case (mergeBuilder, MergeBuilderLogicSCD1(NOTMATCH, None, Some(condition), Some(value))) => mergeBuilder.whenNotMatched(condition).insertExpr(value)
            case (mergeBuilder, MergeBuilderLogicSCD1(NOTMATCH, None, None, Some(value))) => mergeBuilder.whenNotMatched().insertExpr(value)
            case (mergeBuilder, MergeBuilderLogicSCD1(NOTMATCH, None, Some(condition), None)) => mergeBuilder.whenNotMatched(condition).insertAll()
            case (mergeBuilder, MergeBuilderLogicSCD1(NOTMATCH, None, None, None)) => mergeBuilder.whenNotMatched().insertAll()
          }
          case None => deltaMergerBuilder
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
        }
        deltaMergerBuilder.execute()
      }
      else {
        partitionKeys match {
          case Some(value) => updatesDF.write.format("delta").mode("append").partitionBy(value: _*).saveAsTable(targetTableName)
          case None => updatesDF.write.format("delta").mode("append").saveAsTable(targetTableName)
        }
      }
    }
    catch {
      case exception: Exception =>
        println(s"INFO : Inside batch writeSCD1 catch")
        println(exception)
        logger.error(exception.getMessage, exception)
        task match {
          case Some(value) => value.updateAndStoreStatus(PipelineNodeStatus.Error, exception)
          case None => println(exception)
        }

    }

  }


  /**
   *
   * @param queueDf
   * @param targetTableName
   * @param joinKeys
   * @param updateInsertMap
   * @param extraJoinCondition
   * @param partitionKeys
   * @param dedupColumns
   */

  def writeSCD2(spark: SparkSession
                , queueDf: DataFrame
                , targetTableName: String
                , joinKeys: Seq[String]
                , updateInsertMap: MergeBuilderLogicSCD2
                , extraJoinCondition: Option[String]
                , partitionKeys: Option[Seq[String]] = None
                , dedupColumns: Option[Seq[String]] = None
                , task: Option[Task] = None): Unit = {
    try {
      //Removing the duplicates from stream df
      val joinKeysSelect = extractJoinKeys(joinKeys)
      val updatesDF = dedupColumns match {
        case Some(value) =>
          val window = Window.partitionBy(joinKeysSelect: _*).orderBy(value.head, value.tail: _*)
          queueDf.withColumn("rank", row_number().over(window))
            .filter("rank = 1")
            .drop("rank")
        case None => queueDf
      }

      if (checkTable(spark, targetTableName)) {

        val targetTable = DeltaTable.forName(targetTableName)
        var innerJoinConditionsFinal = if (hasColumn(updatesDF, "grouping_jk")) {
          buildInnerJoinCondition(joinKeys :+ "grouping_jk", extraJoinCondition)
        } else {
          buildInnerJoinCondition(joinKeys, extraJoinCondition)
        }

        //add DPP for better merge
        innerJoinConditionsFinal = partitionKeys match {
          case Some(value) => innerJoinConditionsFinal && expr(buildDPPForPartitionKeys(updatesDF, value).replaceAll("and", ""))
          case None => innerJoinConditionsFinal
        }

        println(s"innerJoinConditionsFinal :  $innerJoinConditionsFinal")
        logger.info(s"innerJoinConditionsFinal :  $innerJoinConditionsFinal")


        // Rows to INSERT of existing records
        val newDataToInsert = updatesDF
          .as("updates")
          .join(targetTable.toDF.as("target"), innerJoinConditionsFinal)
          .where(updateInsertMap.matchCondition)
          .selectExpr("updates.*")


        var (union1, union2, mergeJoinCondition) = if (hasColumn(updatesDF, "grouping_jk")) {
          buildMergeJoinCondition(joinKeys :+ "grouping_jk", extraJoinCondition)
        } else {
          buildMergeJoinCondition(joinKeys, extraJoinCondition)
        }

        //add DPP for better merge
        mergeJoinCondition = partitionKeys match {
          case Some(value) => mergeJoinCondition + buildDPPForPartitionKeys(updatesDF, value)
          case None => mergeJoinCondition
        }

        println(s"mergeJoinCondition :  $mergeJoinCondition")
        logger.info(s"mergeJoinCondition :  $mergeJoinCondition")

        // Stage the update by unioning two sets of rows
        // 1. Rows that will be inserted in the whenNotMatched clause
        // 2. Rows that will either update the data of existing records or insert the data of new records
        val stagedUpdates = newDataToInsert
          .selectExpr(union1: _*)
          .unionByName(updatesDF.selectExpr(union2: _*))


        // Apply SCD Type 2 operation using merge
        var deltaMergerBuilder =
          targetTable
            .as("target")
            .merge(
              stagedUpdates.as("updates"), mergeJoinCondition
            )
            .whenMatched(updateInsertMap.matchCondition)
            .updateExpr(updateInsertMap.updateMap)

        deltaMergerBuilder = if (hasColumn(updatesDF, "row_active")) {
          deltaMergerBuilder
            .whenMatched("updates.row_active=false")
            .updateExpr(Map("target.deleted_flag" -> "true"))
        } else {
          deltaMergerBuilder
        }


        deltaMergerBuilder = updateInsertMap match {
          case MergeBuilderLogicSCD2(updateMap, Some(insertMap), matchCondition) => deltaMergerBuilder.whenNotMatched.insertExpr(insertMap)
          case MergeBuilderLogicSCD2(updateMap, None, matchCondition) => deltaMergerBuilder.whenNotMatched.insertAll()
        }
        deltaMergerBuilder.execute()

      } else {

        partitionKeys match {
          case Some(value) => updatesDF.write.format("delta").mode("append").partitionBy(value: _*).saveAsTable(targetTableName)
          case None => updatesDF.write.format("delta").mode("append").saveAsTable(targetTableName)
        }
      }
    }
    catch {
      case exception: Exception =>
        println(s"INFO : Inside batch writeSCD2 catch")
        println(exception)
        logger.error(exception.getMessage, exception)
        task match {
          case Some(value) => value.updateAndStoreStatus(PipelineNodeStatus.Error, exception)
          case None => println(exception)
        }

    }
  }

  /**
   *
   * @param spark
   * @param queueDf
   * @param targetTableName
   * @param joinKeys
   * @param extraJoinCondition
   * @param partitionKeys
   * @param dedupColumns
   */

  def writeSCD4(spark: SparkSession
                , queueDf: DataFrame
                , targetTableName: String
                , joinKeys: Seq[String]
                , extraJoinCondition: Option[String]
                , partitionKeys: Option[Seq[String]] = None
                , dedupColumns: Option[Seq[String]] = None
                , task: Option[Task] = None): Unit = {

    var rawDf: DataFrame = null

    try {

      //Removing the duplicates from stream df
      val joinKeysSelect = extractJoinKeys(joinKeys)
      val updatesDF = dedupColumns match {
        case Some(value) => val window = Window.partitionBy(joinKeysSelect: _*).orderBy(value.head, value.tail: _*)
          queueDf.withColumn("rank", row_number().over(window))
            .filter("rank = 1")
            .drop("rank")
        case None => queueDf
      }


      val currentTable = spark.read.table(targetTableName)
      var conditions = if (hasColumn(updatesDF, "grouping_jk")) {
        buildInnerJoinCondition(joinKeys :+ "grouping_jk", extraJoinCondition) && expr("updates.row_active = true")
      } else {
        buildInnerJoinCondition(joinKeys, extraJoinCondition) && expr("updates.row_active = true")
      }
      //add DPP for better merge
      conditions = partitionKeys match {
        case Some(value) => conditions && expr(buildDPPForPartitionKeys(updatesDF, value).replaceAll("and", ""))
        case None => conditions
      }

      println(s"conditions :  $conditions")
      logger.info(s"conditions :  $conditions")
      //Insert old records in history table
      val histDf = currentTable.as("target")
        .join(updatesDF.as("updates"), conditions)
        .selectExpr("target.*", "updates.updated_at as history_created_at")

      var writerHistory: DataFrameWriter[Row] = {
        if (hasColumn(histDf, "row_active")) {
          histDf.drop("row_active").write.format("delta").mode("append")
        }
        histDf.write.format("delta").mode("append")
      }

      //Check if partition is required for history table
      writerHistory = partitionKeys match {
        case Some(value) => writerHistory.partitionBy(value: _*)
        case None => writerHistory
      }
      writerHistory.saveAsTable(targetTableName + HISTORY_TABLE_TAG)
      //Update or Insert records in current table
      writeSCD1(spark
        , updatesDF
        , targetTableName
        , joinKeys
        , Some(Seq(
          MergeBuilderLogicSCD1("match", None, Some("updates.row_active = false"), Some(Map("target.deleted_flag" -> "true"))),
          MergeBuilderLogicSCD1("match", None, Some("updates.row_active = true"), None),
          MergeBuilderLogicSCD1("notmatch", None, None, None)))
        , extraJoinCondition
        , partitionKeys)
      //Update soft Deleted records in history table
      writeSCD1(spark
        , updatesDF.filter("row_active = false")
        , targetTableName + HISTORY_TABLE_TAG
        , joinKeys
        , Some(Seq(MergeBuilderLogicSCD1("match", None, Some("updates.row_active = false"), Some(Map("target.deleted_flag" -> "true")))))
        , extraJoinCondition
        , partitionKeys)
    }
    catch {
      case exception: Exception =>
        println(s"INFO : Inside batch writeSCD4 catch")
        println(exception)
        logger.error(exception.getMessage, exception)
        task match {
          case Some(value) => value.updateAndStoreStatus(PipelineNodeStatus.Error, exception)
          case None => println(exception)
        }
    }
  }


  /**
   *
   * Stream SCD starts
   *
   *
   *
   *
   *
   */


  /**
   * This function will help in implementing the SCD1 merge logic for stream
   *
   * @param targetTableName    Is the table name to be provided on SCD1 logic has to be applied
   * @param updatesDF          is the minibatch/updates df to be used in SCD1 merge logic
   * @param joinKeys           is the list of join keys can be passed as Seq("id") or Seq("id","name = empName")
   * @param mergeBuilderLogic  It is a Seq of MergeBuilderLogic objects
   * @param extraJoinCondition Can be Some("condition") or None. This will be andition logic for merge join condition
   */

  def writeStreamSCD1(queueDf: DataFrame
                      , targetTableName: String
                      , joinKeys: Seq[String]
                      , mergeBuilderLogic: Option[Seq[MergeBuilderLogicSCD1]]
                      , extraJoinCondition: Option[String]
                      , task: Task
                      , partitionKeys: Option[Seq[String]] = None
                      , dedupColumns: Option[Seq[String]] = None): Unit = {

    var rawDf: DataFrame = null

    try {

      //separate the union queries
      rawDf = queueDf.filter(col("rawDf").isNotNull).select(col("rawDf.*"))
      //Removing the duplicates from stream df
      val joinKeysSelect = extractJoinKeys(joinKeys)
      val updatesDF = dedupColumns match {
        case Some(value) =>
          val window = Window.partitionBy(joinKeysSelect: _*).orderBy(value.head, value.tail: _*)
          queueDf.filter(col("inputStream").isNotNull).select(col("inputStream.*")).withColumn("rank", row_number().over(window))
            .filter("rank = 1")
            .drop("rank")
        case None => queueDf.filter(col("inputStream").isNotNull).select(col("inputStream.*"))
      }

      //generate condition from join keys
      var conditions = if (hasColumn(updatesDF, "grouping_jk")) {
        buildInnerJoinCondition(joinKeys :+ "grouping_jk", extraJoinCondition).toString()
      } else {
        buildInnerJoinCondition(joinKeys, extraJoinCondition).toString()
      }

      //add DPP for better merge
      conditions = partitionKeys match {
        case Some(value) => conditions + buildDPPForPartitionKeys(updatesDF, value)
        case None => conditions
      }

      println(s"conditions :  $conditions")
      logger.info(s"conditions :  $conditions")

      //Build merge logic
      var deltaMergerBuilder = DeltaTable
        .forName(targetTableName)
        .as("target")
        .merge(updatesDF.as("updates"), s"$conditions")

      deltaMergerBuilder = mergeBuilderLogic match {
        case Some(value) => value.foldLeft(deltaMergerBuilder) {

          case (mergeBuilder, MergeBuilderLogicSCD1(MATCH, None, Some(condition), Some(value))) => mergeBuilder.whenMatched(condition).updateExpr(value)
          case (mergeBuilder, MergeBuilderLogicSCD1(MATCH, None, None, Some(value))) => mergeBuilder.whenMatched().updateExpr(value)
          case (mergeBuilder, MergeBuilderLogicSCD1(MATCH, None, Some(condition), None)) => mergeBuilder.whenMatched(condition).updateAll()
          case (mergeBuilder, MergeBuilderLogicSCD1(MATCH, None, None, None)) => mergeBuilder.whenMatched().updateAll()

          case (mergeBuilder, MergeBuilderLogicSCD1(MATCH, Some(DELETE), Some(condition), None)) => mergeBuilder.whenMatched(condition).delete()
          case (mergeBuilder, MergeBuilderLogicSCD1(MATCH, Some(DELETE), None, None)) => mergeBuilder.whenMatched().delete()

          case (mergeBuilder, MergeBuilderLogicSCD1(NOTMATCH, None, Some(condition), Some(value))) => mergeBuilder.whenNotMatched(condition).insertExpr(value)
          case (mergeBuilder, MergeBuilderLogicSCD1(NOTMATCH, None, None, Some(value))) => mergeBuilder.whenNotMatched().insertExpr(value)
          case (mergeBuilder, MergeBuilderLogicSCD1(NOTMATCH, None, Some(condition), None)) => mergeBuilder.whenNotMatched(condition).insertAll()
          case (mergeBuilder, MergeBuilderLogicSCD1(NOTMATCH, None, None, None)) => mergeBuilder.whenNotMatched().insertAll()
        }
        case None => deltaMergerBuilder
          .whenMatched().updateAll()
          .whenNotMatched().insertAll()
      }
      deltaMergerBuilder.execute()


      val currentTimestamp = current_timestamp()
      task.storeStatus(rawDf, currentTimestamp)
      val metricsDf = task.getMetrics(targetTableName)
      task.storeFacts(rawDf.count(), updatesDF.count(), metricsDf, currentTimestamp)

      //get history from delta for latest veriosn collect opertaionMetrics
    }
    catch {
      case exception: Exception => {
        println(exception)
        val currentTimestamp = current_timestamp()
        println(s"INFO : Inside stream writeSCD1 catch")
        task.foreachBatchStoreErrorStatus(rawDf, currentTimestamp, exception)
      }
    }

  }


  /**
   * This function helps in executing the scd2 logic for stream
   *
   * @param updatesDF          is the minibatch/updates df to be used in SCD1 merge logic
   * @param targetTableName    is the target table name
   * @param joinKeys           is the list of join keys can be passed as Seq("id") or Seq("id","name = empName")
   * @param updateInsertMap    It is a Seq of MergeBuilderLogicSCD2 objects
   * @param extraJoinCondition Can be Some("condition") or None. This will be andition logic for merge join condition
   */

  def writeStreamSCD2(queueDf: DataFrame
                      , targetTableName: String
                      , joinKeys: Seq[String]
                      , updateInsertMap: MergeBuilderLogicSCD2
                      , extraJoinCondition: Option[String]
                      , task: Task
                      , partitionKeys: Option[Seq[String]] = None
                      , dedupColumns: Option[Seq[String]] = None): Unit = {
    var rawDf: DataFrame = null
    try {

      //separate the union queries
      rawDf = queueDf.filter(col("rawDf").isNotNull).select(col("rawDf.*"))
      //Removing the duplicates from stream df
      val joinKeysSelect = extractJoinKeys(joinKeys)
      val updatesDF = dedupColumns match {
        case Some(value) =>
          val window = Window.partitionBy(joinKeysSelect: _*).orderBy(value.head, value.tail: _*)
          queueDf.filter(col("inputStream").isNotNull).select(col("inputStream.*")).withColumn("rank", row_number().over(window))
            .filter("rank = 1")
            .drop("rank")
        case None => queueDf.filter(col("inputStream").isNotNull).select(col("inputStream.*"))
      }


      val targetTable = DeltaTable.forName(targetTableName)
      var innerJoinConditionsFinal = if (hasColumn(updatesDF, "grouping_jk")) {
        buildInnerJoinCondition(joinKeys :+ "grouping_jk", extraJoinCondition)
      } else {
        buildInnerJoinCondition(joinKeys, extraJoinCondition)
      }

      //add DPP for better merge
      innerJoinConditionsFinal = partitionKeys match {
        case Some(value) => innerJoinConditionsFinal && expr(buildDPPForPartitionKeys(updatesDF, value).replaceAll("and", ""))
        case None => innerJoinConditionsFinal
      }

      println(s"innerJoinConditionsFinal :  $innerJoinConditionsFinal")
      logger.info(s"innerJoinConditionsFinal :  $innerJoinConditionsFinal")


      // Rows to INSERT of existing records
      val newDataToInsert = updatesDF
        .as("updates")
        .join(targetTable.toDF.as("target"), innerJoinConditionsFinal)
        .where(updateInsertMap.matchCondition)
        .selectExpr("updates.*")


      var (union1, union2, mergeJoinCondition) = if (hasColumn(updatesDF, "grouping_jk")) {
        buildMergeJoinCondition(joinKeys :+ "grouping_jk", extraJoinCondition)
      } else {
        buildMergeJoinCondition(joinKeys, extraJoinCondition)
      }

      //add DPP for better merge
      mergeJoinCondition = partitionKeys match {
        case Some(value) => mergeJoinCondition + buildDPPForPartitionKeys(updatesDF, value)
        case None => mergeJoinCondition
      }

      println(s"mergeJoinCondition :  $mergeJoinCondition")
      logger.info(s"mergeJoinCondition :  $mergeJoinCondition")

      // Stage the update by unioning two sets of rows
      // 1. Rows that will be inserted in the whenNotMatched clause
      // 2. Rows that will either update the data of existing records or insert the data of new records
      val stagedUpdates = newDataToInsert
        .selectExpr(union1: _*)
        .unionByName(updatesDF.selectExpr(union2: _*))


      // Apply SCD Type 2 operation using merge
      var deltaMergerBuilder =
        targetTable
          .as("target")
          .merge(
            stagedUpdates.as("updates"), mergeJoinCondition
          )
          .whenMatched(updateInsertMap.matchCondition)
          .updateExpr(updateInsertMap.updateMap)

      deltaMergerBuilder = if (hasColumn(updatesDF, "row_active")) {
        deltaMergerBuilder
          .whenMatched("updates.row_active=false")
          .updateExpr(Map("target.deleted_flag" -> "true"))
      } else {
        deltaMergerBuilder
      }


      deltaMergerBuilder = updateInsertMap match {
        case MergeBuilderLogicSCD2(updateMap, Some(insertMap), matchCondition) => deltaMergerBuilder.whenNotMatched.insertExpr(insertMap)
        case MergeBuilderLogicSCD2(updateMap, None, matchCondition) => deltaMergerBuilder.whenNotMatched.insertAll()
      }
      deltaMergerBuilder.execute()


      val currentTimestamp = current_timestamp()
      task.storeStatus(rawDf, currentTimestamp)
      val metricsDf = task.getMetrics(targetTableName)
      task.storeFacts(rawDf.count(), updatesDF.count(), metricsDf, currentTimestamp)
    }
    catch {
      case exception: Exception =>
        println(exception)
        val currentTimestamp = current_timestamp()
        println(s"INFO : Inside stream writeSCD2 catch")
        task.foreachBatchStoreErrorStatus(rawDf, currentTimestamp, exception)
    }
  }

  /**
   *
   * @param spark
   * @param queueDf
   * @param targetTableName
   * @param joinKeys
   * @param extraJoinCondition
   * @param task
   * @param partitionKeys
   * @param dedupColumns
   */

  def writeStreamSCD4(spark: SparkSession
                      , queueDf: DataFrame
                      , targetTableName: String
                      , joinKeys: Seq[String]
                      , extraJoinCondition: Option[String]
                      , task: Task
                      , partitionKeys: Option[Seq[String]] = None
                      , dedupColumns: Option[Seq[String]] = None): Unit = {

    var rawDf: DataFrame = null

    try {

      //separate the union queries
      rawDf = queueDf.filter(col("rawDf").isNotNull).select(col("rawDf.*"))
      //Removing the duplicates from stream df
      val joinKeysSelect = extractJoinKeys(joinKeys)
      val updatesDF = dedupColumns match {
        case Some(value) => val window = Window.partitionBy(joinKeysSelect: _*).orderBy(value.head, value.tail: _*)
          queueDf.filter(col("inputStream").isNotNull).select(col("inputStream.*")).withColumn("rank", row_number().over(window))
            .filter("rank = 1")
            .drop("rank")
        case None => queueDf.filter(col("inputStream").isNotNull).select(col("inputStream.*"))
      }


      val currentTable = spark.read.table(targetTableName)
      var conditions = if (hasColumn(updatesDF, "grouping_jk")) {
        buildInnerJoinCondition(joinKeys :+ "grouping_jk", extraJoinCondition) && expr("updates.row_active = true")
      } else {
        buildInnerJoinCondition(joinKeys, extraJoinCondition) && expr("updates.row_active = true")
      }
      //add DPP for better merge
      conditions = partitionKeys match {
        case Some(value) => conditions && expr(buildDPPForPartitionKeys(updatesDF, value).replaceAll("and", ""))
        case None => conditions
      }

      println(s"conditions :  $conditions")
      logger.info(s"conditions :  $conditions")


      //Insert old records in history table
      val histDf = currentTable.as("target")
        .join(updatesDF.as("updates"), conditions)
        .selectExpr("target.*", "updates.updated_at as history_created_at")
      var writerHistory: DataFrameWriter[Row] = histDf.write.format("delta").mode("append")
      writerHistory = partitionKeys match {
        case Some(value) => writerHistory.partitionBy(value: _*)
        case None => writerHistory
      }
      writerHistory.saveAsTable(targetTableName + HISTORY_TABLE_TAG)


      //Update or Insert records in current table
      writeSCD1(spark
        , updatesDF
        , targetTableName
        , joinKeys
        , Some(Seq(
          MergeBuilderLogicSCD1("match", None, Some("updates.row_active = false"), Some(Map("target.deleted_flag" -> "true"))),
          MergeBuilderLogicSCD1("match", None, Some("updates.row_active = true"), None),
          MergeBuilderLogicSCD1("notmatch", None, None, None)))
        , extraJoinCondition
        , partitionKeys)


      //Update soft Deleted records in history table
      writeSCD1(spark
        , updatesDF.filter("row_active = false")
        , targetTableName + HISTORY_TABLE_TAG
        , joinKeys
        , Some(Seq(MergeBuilderLogicSCD1("match", None, Some("updates.row_active = false"), Some(Map("target.deleted_flag" -> "true")))))
        , extraJoinCondition
        , partitionKeys)

      val currentTimestamp = current_timestamp()
      task.storeStatus(rawDf, currentTimestamp)
      val metricsDf = task.getMetrics(targetTableName)
      task.storeFacts(rawDf.count(), updatesDF.count(), metricsDf, currentTimestamp)
    }
    catch {
      case exception: Exception =>
        println(exception)
        val currentTimestamp = current_timestamp()
        println(s"INFO : Inside stream writeSCD4 catch")
        task.foreachBatchStoreErrorStatus(rawDf, currentTimestamp, exception)
    }

  }

}
