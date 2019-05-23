package com.pharbers.data.aggregation

import com.pharbers.data.aggregation.functions._
import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.data.util._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat}

case class MaxResultCompareResultAgg(args: MapArgs) extends sequenceJobWithMap {

    override val name: String = ""

    val mongoDbSource: String = args.get.getOrElse("mongoDbSource", throw new Exception("not found productMongo")).getBy[StringArgs]
    val productMongo: String = args.get.getOrElse("productMongo", throw new Exception("not found productMongo")).getBy[StringArgs]
    val topN: Int = args.get.getOrElse("topN", throw new Exception("not found productMongo")).getBy[StringArgs].toInt

    val salesRankOtherAggFunctions: List[DataFrame => DataFrame] = List(
        distinguishRankTopNAndOther("SALES_RANK", topN, "PRODUCT_NAME", "CORP_NAME", "MIN_PRODUCT"),
        aggByGroups(maxResultAggBySalesRankOtherAndYmsExpr, "MARKET", "YM", "YM_TYPE", "SALES_RANK", "ADDRESS", "ADDRESS_TYPE"),
        df => df.withColumn("PRODUCT", concat(col("PRODUCT_NAME"), col("CORP_NAME"), col("ADDRESS"))),
        groupByXYGroup("YM", "PRODUCT"),
        eiCompare("SALES"),
        yearGRCompare("SALES"),
        yearGRCompare("SALES_SOM")
    )
    override val actions: List[pActionTrait] = List(
        CompareMaxResultAggByFunctions(Map("functions" -> salesRankOtherAggFunctions, "name" -> "rankTopNAndOther", "source" -> "maxResultCompare"))
    )

    override def perform(pr: pActionArgs): pActionArgs = {
        val maxResultCompare = Mongo2DF(mongoDbSource)
        val dfMap = super.perform(MapArgs(Map("maxResultCompare" -> DFArgs(maxResultCompare)))).asInstanceOf[MapArgs].get

        dfMap("rankTopNAndOther").get.asInstanceOf[DataFrame]
                .save2Mongo(productMongo)

        MapArgs(Map(
            "result" -> StringArgs("Conversion success")
        ))
    }
}
