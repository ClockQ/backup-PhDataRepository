package com.pharbers.data.job.aggregationJob

import com.pharbers.data.conversion.{HospConversion, MaxResultConversion, ProductDevConversion}
import com.pharbers.data.util.ParquetLocation._
import com.pharbers.data.util._
import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.util.log.phLogTrait.phDebugLog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class MaxResultAggregationForMonthJob(args: Map[String, String])(implicit any: Any = null) extends sequenceJobWithMap {
    override val actions: List[pActionTrait] = Nil
    override val name: String = "max result aggregation by month"

    val maxResultERDLocation: String = args("max_result_erd_location")
    val ym: Seq[Int] = args("ym").split(",").map(x => x.toInt)
    val companyId: String = args("company")
    val months: Seq[String] = args("months").split(",")

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

        val maxMiddleDF = maxDIS
                .select(col("COMPANY_ID"), col("province-name"), col("city-name")
                    , col("MIN_PRODUCT"), col("YM"), col("SALES")
                    , col("UNITS"), col("MARKET"), col("PRODUCT_NAME")
                    , col("MOLE_NAME"), col("CORP_NAME"), col("PH_CORP_NAME"))
                .withColumnRenamed("province-name", "province")
                .withColumnRenamed("city-name", "city")
                .addMonth
                .filter(col("COMPANY_ID") === companyId
                        && col("YM") >= ym.min
                        && col("YM") <= ym.max
                )

                months.foreach(x => {
                    val oneMonthAgg = maxMiddleDF
                            .filter(col("MONTH") === x)
                            .groupBy("MIN_PRODUCT", "YM") // 消除了地区维度
                            .agg(expr("count(province) as PROVINCE_COUNT"),
                                expr("count(city) as CITY_COUNT"),
                                expr("sum(SALES) as SALES"),
                                expr("sum(UNITS) as UNITS"),
                                expr("first(COMPANY_ID) as COMPANY_ID"),
                                expr("first(PRODUCT_NAME) as PRODUCT_NAME"),
                                expr("first(MOLE_NAME) as MOLE_NAME"),
                                expr("first(CORP_NAME) as CORP_NAME"),
                                expr("first(PH_CORP_NAME) as PH_CORP_NAME"))
                            .withColumn("time", unix_timestamp)
                    phDebugLog(s"消除了地区,聚合$x 月 maxAggDF完成")
                    oneMonthAgg.save2Parquet(MAX_RESULT_ADDRESS_AGG_LOCATION)
                })

//        val dfMap = super.perform(MapArgs(Map("productDF" -> DFArgs(productDF)))).asInstanceOf[MapArgs].get
//
//        val productAggregationDF = Parquet2DF(dfMap("ProductAgg").get.asInstanceOf[String])
//
//        productAggregationDF.save2Mongo("ProductAgg")


        MapArgs(Map(
            "result" -> StringArgs("Conversion success")
        ))
    }
}
