package com.pharbers.data.job

import com.pharbers.data.conversion.{HospConversion, MaxResultConversion, ProductDevConversion}
import com.pharbers.data.job.AggregationJob.{MarketAggregationJob, ProductAggregationJob}
import com.pharbers.data.util.ParquetLocation._
import com.pharbers.data.util._
import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.util.log.phLogTrait.phDebugLog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

case class maxResultAggregationJob(args: Map[String, String])(implicit any: Any = null) extends sequenceJobWithMap {
    override val actions: List[pActionTrait] = List(MarketAggregationJob(args),ProductAggregationJob(args))
    override val name: String = "max result aggregation"

    val maxResultERDLocation: String = args("max_result_erd_location")
    val ym: Seq[Int] = args("ym").split(",").map(x => x.toInt)
    val companyId: String = args("company")

    val hospCvs = HospConversion()
//    val prodCvs = ProdConversion()
    val pfizerInfMaxCvs = MaxResultConversion(companyId)
    val PROD_DEV_CVS = ProductDevConversion()

    val maxResultERD: DataFrame = Parquet2DF(maxResultERDLocation)

    val hospDIS: DataFrame = hospCvs.toDIS(
        Map(
            "hospBaseERD" -> Parquet2DF(HOSP_BASE_LOCATION),
            "hospAddressERD" -> Parquet2DF(HOSP_ADDRESS_BASE_LOCATION),
            "hospPrefectureERD" -> Parquet2DF(HOSP_ADDRESS_PREFECTURE_LOCATION),
            "hospCityERD" -> Parquet2DF(HOSP_ADDRESS_CITY_LOCATION),
            "hospProvinceERD" -> Parquet2DF(HOSP_ADDRESS_PROVINCE_LOCATION)
        )
    )("hospDIS")
    val productDIS: DataFrame = PROD_DEV_CVS.toDIS(
            Map(
                "productDevERD" -> Parquet2DF(PROD_DEV_LOCATION),
                "productEtcERD" -> Parquet2DF(PROD_ETC_LOCATION + "/" + companyId),
                "productImsERD" -> Parquet2DF(PROD_IMS_LOCATION)
            )
        )("productDIS")

    val maxDIS: DataFrame = pfizerInfMaxCvs.toDIS(
        Map(
            "maxERD" -> maxResultERD,
            "hospDIS" -> hospDIS,
            "prodDIS" -> productDIS,
            "sourceERD" -> CSV2DF(SOURCE_LOCATION)
        )
    )("maxDIS")


    override def perform(pr: pActionArgs): pActionArgs = {
        phDebugLog("聚合开始:" + maxResultERDLocation)
        val marketDF = maxDIS
                .select(col("COMPANY_ID"), col("province-name"), col("city-name")
                    , col("MIN_PRODUCT"), col("YM"),  col("SALES")
                    , col("UNITS"), col("MARKET"))
                .withColumnRenamed("province-name", "province")
                .withColumnRenamed("city-name", "city")
                .filter(col("COMPANY_ID") === companyId && col("YM") >= ym.min && col("YM") <= ym.max)
                .groupBy("MARKET", "YM")
                .agg(expr("count(province) as PROVINCE_COUNT"),
                    expr("count(city) as CITY_COUNT"),
                    expr("count(MIN_PRODUCT) as PRODUCT_COUNT"),
                    expr("sum(SALES) as SALES"),
                    expr("sum(UNITS) as UNITS"),
                    expr("first(COMPANY_ID) as COMPANY_ID"))
                .cache()

        phDebugLog("marketDF完成")

        val productDF = maxDIS
                .select(col("COMPANY_ID"), col("province-name"), col("city-name")
                    , col("MIN_PRODUCT"), col("YM"), col("SALES")
                    , col("UNITS"), col("MARKET"), col("PRODUCT_NAME")
                    , col("MOLE_NAME"), col("CORP_NAME"), col("PH_CORP_NAME"))
                .withColumnRenamed("province-name", "province")
                .withColumnRenamed("city-name", "city")
                .filter(col("COMPANY_ID") === companyId
                        && col("YM") >= ym.min
                        && col("YM") <= ym.max
//                        && col("MARKET") === market
                )
                .groupBy("MIN_PRODUCT", "YM", "MARKET")
                .agg(expr("count(province) as PROVINCE_COUNT"),
                    expr("count(city) as CITY_COUNT"),
                    expr("sum(SALES) as SALES"),
                    expr("sum(UNITS) as UNITS"),
                    expr("first(COMPANY_ID) as COMPANY_ID"),
                    expr("first(PRODUCT_NAME) as PRODUCT_NAME"),
                    expr("first(MOLE_NAME) as MOLE_NAME"),
                    expr("first(CORP_NAME) as CORP_NAME"),
                    expr("first(PH_CORP_NAME) as PH_CORP_NAME"))

        phDebugLog("productDF完成")

        val dfMap = super.perform(MapArgs(Map("marketDF" -> DFArgs(marketDF), "productDF" -> DFArgs(productDF)))).asInstanceOf[MapArgs].get

        val marketAggregationDF = Parquet2DF(dfMap("MarketAgg").get.asInstanceOf[String])
        val productAggregationDF = Parquet2DF(dfMap("ProductAgg").get.asInstanceOf[String])
//        phDebugLog("MarketAggregation:" + marketAggregationDF.count())
//        phDebugLog("ProductAggregation:" + productAggregationDF.count())

        val MarketdimensionDF = productAggregationDF.filter(col("MIN_PRODUCT") === "top10")
                        .selectExpr("MARKET as top10MARKET", "YM as topYM", "SALES as CONCENTRATED_SALES"
                            , "SALES_SOM as CONCENTRATED_SOM", "SALES_RING_GROWTH as CONCENTRATED_RING_GROWTH", "SALES_YEAR_GROWTH as CONCENTRATED_YEAR_GROWTH")
                        .join(marketAggregationDF, col("top10MARKET") === col("MARKET")
                            && col("topYM") === col("YM"), "right")
                        .drop("top10MARKET", "topYM")


        MarketdimensionDF.save2Mongo("MarketAgg")
        productAggregationDF.save2Mongo("ProductAgg")


        MapArgs(Map(
            "result" -> StringArgs("Conversion success")
        ))
    }
}
