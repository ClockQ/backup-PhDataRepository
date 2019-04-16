package com.pharbers.data.job.AggregationJob

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, pActionArgs, pActionTrait}
import com.pharbers.pactions.jobs.sequenceJobWithMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.pharbers.data.job.AggregationJob.util.util._
import com.pharbers.data.util._
import com.pharbers.util.log.phLogTrait.phDebugLog

case class ProductAggregationJob(args: Map[String, String]) extends sequenceJobWithMap {
    override val actions: List[pActionTrait] = Nil
    override val name: String = " ProductAggregation"

//    val ym: Seq[Int] = args("ym").split("#").map(x => x.toInt)
    val market: String = args("market")

    override def perform(pr: pActionArgs): pActionArgs = {
        val productDF: DataFrame = pr.asInstanceOf[MapArgs].get("productDF").asInstanceOf[DFArgs].get
        val productTrendDF = productDF.som("TIME", "MIN_PRODUCT", "SALES", col("MARKET"))
                .ringGR("TIME", "MIN_PRODUCT", "SALES", col("MARKET"))
                .addRank("TIME", "SALES", col("MARKET"))
                .addRank("TIME", "RING_GROWTH", col("MARKET"))
                .ringGR("TIME", "MIN_PRODUCT", "SALES", col("MARKET"))

        phDebugLog("productTrendDF完成")

        val productCompositionDF = productTrendDF
                .withColumn("COMPOSITION", when(col("SALES_RANK") > 5, "others").otherwise(col("SALES_RANK")))
                .withColumn("MIN_PRODUCT", when(col("SALES_RANK") > 5, "others").otherwise(col("MIN_PRODUCT")))
                .select("COMPANY_ID", "MIN_PRODUCT", "TIME", "SALES_SOM", "COMPOSITION", "SALES", "MARKET")
                .groupBy("COMPOSITION", "TIME", "MARKET")
                .agg(expr("sun(SALES_SOM) as SALES_SOM"),
                    expr("sun(SALES) as SALES"))
                .alignAt(productTrendDF)

        phDebugLog("productAggregationDf完成")
//        val marketConcentrationRateDF = productTrendDF.yearGR("TIME", "MIN_PRODUCT", "SALES")
//                .ringGR("TIME", "MIN_PRODUCT", "SALES")
//                .select("COMPANY_ID", "MIN_PRODUCT", "TIME", "MIN_PRODUCT_SOM", "YEAR_GROWTH", "RING_GROWTH", "SALES_RANK", "SALES")
//                .filter(col("TIME").isin(ym: _*))

        DFArgs(productTrendDF union productCompositionDF)
    }
}
