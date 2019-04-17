package com.pharbers.data.job.AggregationJob

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, pActionArgs, pActionTrait}
import com.pharbers.pactions.jobs.sequenceJobWithMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.pharbers.util.log.phLogTrait.phDebugLog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.IntegerType

case class MarketAggregationJob(args: Map[String, String]) extends sequenceJobWithMap {
    override val actions: List[pActionTrait] = Nil
    override val name: String = "MarketAgg"

//    val ym: Seq[Int] = args("ym").split("#").map(x => x.toInt)
    val market: String = args("market")

    override def perform(pr: pActionArgs): pActionArgs = {
        import com.pharbers.data.job.AggregationJob.util._

        val marketDF: DataFrame = pr.asInstanceOf[MapArgs].get("marketDF").asInstanceOf[DFArgs].get

        val marketAggregationDf = marketDF
//                .withColumn("YEAR_ON_YEAR", first(col("PRODUCT_COUNT")).over(windowYearOnYear))
//                .withColumn( "YEAR_GROWTH", (col("YEAR_ON_YEAR") - col("PRODUCT_COUNT")) / col("YEAR_ON_YEAR"))
                .yearGR("YM", "PRODUCT_COUNT", col("MARKET"))
                .ringGR("YM", "PRODUCT_COUNT", col("MARKET"))
                .som("MARKET", "SALES", col("YM"))
                .yearGR("YM", "SALES_SOM", col("MARKET"))
                .ringGR("YM", "SALES_SOM", col("MARKET"))


        phDebugLog("marketAggregationDf完成")
//        val marketSizeTrendDF = marketDF.som("YM", "MARKET", "SALES")
//                .sort(col("YM"))
//                .select("COMPANY_ID","MARKET", "YM", "MARKET_SOM")

//        val  marketSizeGrowthDF = marketSizeTrendDF.yearGR("MARKET", "YM", "MARKET_SOM")
//                .ringGR("MARKET", "YM", "MARKET_SOM")
////                .select("COMPANY_ID","MARKET", "YM", "MARKET_SOM", "YEAR_GROWTH", "RING_GROWTH")


        DFArgs(marketAggregationDf)
//        MapArgs(Map("productCount" -> DFArgs(productCountDf), "marketSizeTrend" -> DFArgs(marketSizeTrendDF), "marketSizeGrowth" -> DFArgs(marketSizeGrowthDF)))
    }
}
