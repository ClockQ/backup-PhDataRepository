package com.pharbers.data.job.AggregationJob

import java.util.UUID

import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.jobs.sequenceJobWithMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.pharbers.util.log.phLogTrait.phDebugLog

case class ProductAggregationJob(args: Map[String, String]) extends sequenceJobWithMap {
    override val actions: List[pActionTrait] = Nil
    override val name: String = "ProductAgg"

//    val ym: Seq[Int] = args("ym").split("#").map(x => x.toInt)
//    val market: String = args("market")

    override def perform(pr: pActionArgs): pActionArgs = {
        import com.pharbers.data.util.PhWindowUtil._
        import com.pharbers.data.util._
        import com.pharbers.data.util.ParquetLocation._

        val productDF: DataFrame = pr.asInstanceOf[MapArgs].get("productDF").asInstanceOf[DFArgs].get
        val productTrendDF = productDF.addSom( "SALES", col("MARKET"), col("YM"))
                .addRingGR("YM", "SALES", col("MARKET"), col("MIN_PRODUCT"))
                .addRank("SALES", col("MARKET"), col("YM"))
                .addRank("SALES_RING_GROWTH", col("MARKET"), col("YM"))
                .addRingGR("YM", "SALES", col("MARKET"), col("MIN_PRODUCT"))
                .addYearGR("YM", "SALES", col("MARKET"), col("MIN_PRODUCT"))


        phDebugLog("productTrendDF完成")

        val productCompositionDF = productTrendDF
                .withColumn("COMPOSITION", when(col("SALES_RANK") > 5, "others").otherwise(col("SALES_RANK")))
                .filter(col("COMPOSITION") === "other")
                .withColumn("MIN_PRODUCT", lit("other"))
                .select("COMPANY_ID", "MIN_PRODUCT", "YM", "SALES_SOM", "COMPOSITION", "SALES", "MARKET","PROVINCE_COUNT", "CITY_COUNT", "UNITS")
                .groupBy("YM", "MARKET")
                .agg(expr("sum(SALES_SOM) as SALES_SOM"),
                    expr("sum(SALES) as SALES"),
                    expr("sum(PROVINCE_COUNT) as PROVINCE_COUNT"),
                    expr("sum(CITY_COUNT) as CITY_COUNT"),
                    expr("sum(UNITS) as UNITS"))
                .drop("COMPOSITION")
                .alignAt(productTrendDF)

        val marketCompositionDF = productTrendDF
                .withColumn("COMPOSITION", when(col("SALES_RANK") < 11, "top10").otherwise(lit("low")))
                .withColumn("MIN_PRODUCT", col("COMPOSITION"))
                .select("COMPANY_ID", "MIN_PRODUCT", "YM", "SALES_SOM", "COMPOSITION", "SALES", "MARKET","PROVINCE_COUNT", "CITY_COUNT", "UNITS")
                .groupBy("YM", "MARKET", "MIN_PRODUCT")
                .agg(expr("sum(SALES_SOM) as SALES_SOM"),
                    expr("sum(SALES) as SALES"),
                    expr("sum(PROVINCE_COUNT) as PROVINCE_COUNT"),
                    expr("sum(CITY_COUNT) as CITY_COUNT"),
                    expr("sum(UNITS) as UNITS"))
                .drop("COMPOSITION")
                .addYearGR("YM", "SALES", col("MARKET"), col("MIN_PRODUCT"))
                .addRingGR("YM", "SALES", col("MARKET"), col("MIN_PRODUCT"))
                .filter(col("MIN_PRODUCT") === "top10")
                .alignAt(productTrendDF)

//        val marketConcentrationRateDF = productTrendDF.yearGR("YM", "MIN_PRODUCT", "SALES")
//                .ringGR("YM", "MIN_PRODUCT", "SALES")
//                .select("COMPANY_ID", "MIN_PRODUCT", "YM", "MIN_PRODUCT_SOM", "YEAR_GROWTH", "RING_GROWTH", "SALES_RANK", "SALES")
//                .filter(col("YM").isin(ym: _*))


        val uuid = UUID.randomUUID().toString
        val productAgg = productTrendDF unionByName productCompositionDF unionByName marketCompositionDF
        productAgg.save2Parquet(MAX_RESULT_PRODUCT_AGG_LOCATION + "/" + uuid)
        phDebugLog("productAggregationDf完成")
        StringArgs(MAX_RESULT_PRODUCT_AGG_LOCATION + "/" + uuid)
    }
}
