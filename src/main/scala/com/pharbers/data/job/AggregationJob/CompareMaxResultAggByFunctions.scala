package com.pharbers.data.job.aggregationJob

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, pActionArgs, pActionTrait}
import com.pharbers.pactions.jobs.sequenceJobWithMap
import org.apache.spark.sql.DataFrame

case class CompareMaxResultAggByFunctions(args: Map[String, Any]) extends sequenceJobWithMap {
    val DfFunctions: Seq[DataFrame => DataFrame] = args("functions").asInstanceOf[Seq[DataFrame => DataFrame]]
    val source: String = args("source").asInstanceOf[String]

    override val actions: List[pActionTrait] = Nil
    override val name: String = args("name").asInstanceOf[String]

    override def perform(pr: pActionArgs): pActionArgs = {

        val resultDF = DfFunctions.foldLeft(pr.asInstanceOf[MapArgs].get(source).asInstanceOf[DFArgs].get)((left, right) => right(left))

        DFArgs(resultDF)
    }
}
