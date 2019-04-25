//package com.pharbers.data.job.AggregationJob
//
//import com.pharbers.pactions.actionbase.{pActionArgs, pActionTrait}
//import com.pharbers.pactions.jobs.sequenceJobWithMap
//import org.apache.spark.sql.DataFrame
//
//class CompareMaxResultAggByProduct(args: Map[String, Any]) extends sequenceJobWithMap {
//    override val actions: List[pActionTrait] = Nil
//    override val name: String = "CompareMaxResultAggByProduct"
//
//    val groupByProductFunction: DataFrame => DataFrame = args("groupByProductFunction").asInstanceOf[DataFrame => DataFrame]
//
//    override def perform(pr: pActionArgs): pActionArgs = {
//
//    }
//}
