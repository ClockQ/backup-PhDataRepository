package com.pharbers.data.job.AggregationJob

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, pActionArgs, pActionTrait}
import com.pharbers.pactions.jobs.sequenceJobWithMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.pharbers.data.job.AggregationJob.util.util._
import com.pharbers.util.log.phLogTrait.phDebugLog

case class MarketAggregationJob(args: Map[String, String]) extends sequenceJobWithMap {
    override val actions: List[pActionTrait] = Nil
    override val name: String = "MarketAggregation"

//    val ym: Seq[Int] = args("ym").split("#").map(x => x.toInt)
    val market: String = args("market")

    override def perform(pr: pActionArgs): pActionArgs = {
        val marketDF: DataFrame = pr.asInstanceOf[MapArgs].get("marketDF").asInstanceOf[DFArgs].get
        val marketAggregationDf = marketDF.yearGR("MARKET", "TIME", "PRODUCT_COUNT")
                .ringGR("MARKET", "TIME", "PRODUCT_COUNT")
                .som("TIME", "MARKET", "SALES")
                .yearGR("MARKET", "TIME", "SALES_SOM")
                .ringGR("MARKET", "TIME", "SALES_SOM")
                .select("COMPANY_ID","MARKET", "TIME", "PRODUCT_COUNT", "PRODUCT_COUNT_YEAR_GROWTH", "PRODUCT_COUNT_RING_GROWTH")

        phDebugLog("marketAggregationDf完成")
//        val marketSizeTrendDF = marketDF.som("TIME", "MARKET", "SALES")
//                .sort(col("TIME"))
//                .select("COMPANY_ID","MARKET", "TIME", "MARKET_SOM")

//        val  marketSizeGrowthDF = marketSizeTrendDF.yearGR("MARKET", "TIME", "MARKET_SOM")
//                .ringGR("MARKET", "TIME", "MARKET_SOM")
////                .select("COMPANY_ID","MARKET", "TIME", "MARKET_SOM", "YEAR_GROWTH", "RING_GROWTH")


        DFArgs(marketAggregationDf)
//        MapArgs(Map("productCount" -> DFArgs(productCountDf), "marketSizeTrend" -> DFArgs(marketSizeTrendDF), "marketSizeGrowth" -> DFArgs(marketSizeGrowthDF)))
    }
}
