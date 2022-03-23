package com.databricks.genericPipelineOrchestrator.writter

import com.databricks.genericPipelineOrchestrator.commons.{Options, Task}
import com.databricks.genericPipelineOrchestrator.writter.config.WriteStreamConfig
import org.apache.spark.sql.DataFrame

abstract class BaseWritter extends  Task{
    var options:Options = null
    var sourceDF :DataFrame = null
    var sourceInMemoryTableName :String = null

}

/**
 *
 * @param condtionType Can be either match or notmatch
 * @param deleteOption Can be Some("delete") or None
 * @param condition Cane be Some("condition") or None
 * @param updateInsertMap Can be Some(Map("target.id" -> "updates.id", "target.address" -> "updates.address")) or None
 */
case class MergeBuilderLogicSCD1(condtionType: String, deleteOption: Option[String] = None, condition: Option[String] = None, updateInsertMap : Option[Map[String,String]] = None)

/**
 *
 * @param updateMap Map of the columns to be updated
 * @param insertMap Map of the columns to be inserted. It is optional. incase of None value all columns are inserted
 * @param matchCondition This match condition helps for update
 */
case class MergeBuilderLogicSCD2(updateMap: Map[String,String], insertMap: Option[Map[String,String]] = None, matchCondition: String)


case class mergeSCD1Options(outputStreamConf: WriteStreamConfig, MergeBuilderLogic: Option[Seq[MergeBuilderLogicSCD1]], joinKeys: Seq[String], partitionKeys: Option[Seq[String]], dedupKeys: Option[Seq[String]], extraJoinCondition: Option[String])
case class mergeSCD2Options(outputStreamConf: WriteStreamConfig, MergeBuilderLogic: MergeBuilderLogicSCD2, joinKeys: Seq[String], partitionKeys: Option[Seq[String]], dedupKeys: Option[Seq[String]], extraJoinCondition: Option[String])
case class mergeSCD4Options(outputStreamConf: WriteStreamConfig, joinKeys: Seq[String], partitionKeys: Option[Seq[String]], dedupKeys: Option[Seq[String]], extraJoinCondition: Option[String])
