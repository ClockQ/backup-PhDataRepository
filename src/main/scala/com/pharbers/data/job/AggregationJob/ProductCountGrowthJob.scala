//package com.pharbers.data.job.AggregationJob
//
//import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, pActionArgs, pActionTrait}
//import com.pharbers.pactions.jobs.sequenceJobWithMap
//import org.apache.spark.sql.DataFrame
//import com.pharbers.data.job.AggregationJob.util._
//import org.apache.spark.sql.functions._
//
//class ProductCountGrowthJob(args: Map[String, String]) extends sequenceJobWithMap {
//    override val actions: List[pActionTrait] = Nil
//    override val name: String = "ProductCountGrowth"
//
//    val ym: Seq[Int] = args("ym").split("#").map(x => x.toInt)
//    val market: String = args("market")
//
//    override def perform(pr: pActionArgs): pActionArgs = {
//        val marketDF: DataFrame = pr.asInstanceOf[MapArgs].get("marketDF").asInstanceOf[DFArgs].get
//        val productCountDf = marketDF.yearGR("MARKET", "TIME", "PRODUCT_COUNT")
//                .ringGR("MARKET", "TIME", "PRODUCT_COUNT")
//                .select("COMPANY_ID","MARKET", "TIME", "PRODUCT_COUNT", "YEAR_GROWTH", "RING_GROWTH")
//                .filter(col("TIME").isin(ym:_*))
//        DFArgs(productCountDf)
//    }
//}
