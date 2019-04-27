package com.pharbers.data.job.aggregationJob

import java.util.UUID

import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.jobs.sequenceJobWithMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.pharbers.util.log.phLogTrait.phDebugLog

case class MarketAggregationJob(args: Map[String, String]) extends sequenceJobWithMap {
    override val actions: List[pActionTrait] = Nil
    override val name: String = "MarketAgg"

//    val ym: Seq[Int] = args("ym").split("#").map(x => x.toInt)
//    val market: String = args("market")

    override def perform(pr: pActionArgs): pActionArgs = {
        import com.pharbers.data.util.PhWindowUtil._
        import com.pharbers.data.util._
        import com.pharbers.data.util.ParquetLocation._

        val marketDF: DataFrame = pr.asInstanceOf[MapArgs].get("marketDF").asInstanceOf[DFArgs].get

        val marketAggregationDf = marketDF
//                .withColumn("YEAR_ON_YEAR", first(col("PRODUCT_COUNT")).over(windowYearOnYear))
//                .withColumn( "YEAR_GROWTH", (col("YEAR_ON_YEAR") - col("PRODUCT_COUNT")) / col("YEAR_ON_YEAR"))
                .addYearGR("YM", "PRODUCT_COUNT", col("MARKET"))
                .addRingGR("YM", "PRODUCT_COUNT", col("MARKET"))
                .addSom("SALES", col("YM"))
                .addYearGR("YM", "SALES_SOM", col("MARKET"))
                .addRingGR("YM", "SALES_SOM", col("MARKET"))


//        val marketSizeTrendDF = marketDF.som("YM", "MARKET", "SALES")
//                .sort(col("YM"))
//                .select("COMPANY_ID","MARKET", "YM", "MARKET_SOM")

//        val  marketSizeGrowthDF = marketSizeTrendDF.yearGR("MARKET", "YM", "MARKET_SOM")
//                .ringGR("MARKET", "YM", "MARKET_SOM")
////                .select("COMPANY_ID","MARKET", "YM", "MARKET_SOM", "YEAR_GROWTH", "RING_GROWTH")

        val uuid = UUID.randomUUID().toString
        marketAggregationDf.save2Parquet(MAX_RESULT_MARKET_AGG_LOCATION + "/" + uuid)
        phDebugLog("marketAggregationDf完成")
        StringArgs(MAX_RESULT_MARKET_AGG_LOCATION + "/" + uuid)
//        MapArgs(Map("productCount" -> DFArgs(productCountDf), "marketSizeTrend" -> DFArgs(marketSizeTrendDF), "marketSizeGrowth" -> DFArgs(marketSizeGrowthDF)))
    }
}
