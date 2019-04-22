//package com.pharbers.data.job.AggregationJob
//
//import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, pActionArgs, pActionTrait}
//import com.pharbers.pactions.jobs.sequenceJobWithMap
//import org.apache.spark.sql.DataFrame
//import org.apache.spark.sql.expressions.Window
//import org.apache.spark.sql.functions._
//import com.pharbers.data.job.AggregationJob.util._
//
//class MarketSizeGrowthJob(args: Map[String, String]) extends sequenceJobWithMap {
//    override val actions: List[pActionTrait] = Nil
//    override val name: String = "MarketSizeGrowth"
//
//    val ym: Seq[Int] = args("ym").split("#").map(x => x.toInt)
//    val market: String = args("market")
//
//    override def perform(pr: pActionArgs): pActionArgs = {
//        val TrendDF: DataFrame = pr.asInstanceOf[MapArgs].get("MarketSizeTrend").asInstanceOf[DFArgs].get
//        val GrowthDF = TrendDF.yearGR("MARKET", "TIME", "MARKET_SIZE")
//                .ringGR("MARKET", "TIME", "MARKET_SIZE")
//                .select("COMPANY_ID","MARKET", "TIME", "MARKET_SIZE", "YEAR_GROWTH", "RING_GROWTH")
//                .filter(col("TIME").isin(ym:_*))
//
//        DFArgs(GrowthDF)
//    }
//}
