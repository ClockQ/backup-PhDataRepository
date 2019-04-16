package com.pharbers.data.job

import com.pharbers.data.conversion.{HospConversion, MaxResultConversion, ProdConversion}
import com.pharbers.data.job.AggregationJob.{MarketAggregationJob, ProductAggregationJob}
import com.pharbers.data.util.ParquetLocation._
import com.pharbers.data.util._
import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.util.log.phLogTrait.phDebugLog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class maxResultAggregationJob(args: Map[String, String])(implicit any: Any = null) extends sequenceJobWithMap {
    override val actions: List[pActionTrait] = List(MarketAggregationJob(args),ProductAggregationJob(args))
    override val name: String = "max result aggregation"

    val maxResultERDLocation: String = args("max_result_erd_location")
    val ym: Seq[Int] = args("ym").split("#").map(x => x.toInt)
    val market: String = args("market")
    val companyId: String = args("company")

    val hospCvs = HospConversion()
    val prodCvs = ProdConversion()
    val pfizerInfMaxCvs = MaxResultConversion(companyId)

    val hospDIS: DataFrame = hospCvs.toDIS(
        Map(
            "hospBaseERD" -> Parquet2DF(HOSP_BASE_LOCATION),
            "hospAddressERD" -> Parquet2DF(HOSP_ADDRESS_BASE_LOCATION),
            "hospPrefectureERD" -> Parquet2DF(HOSP_ADDRESS_PREFECTURE_LOCATION),
            "hospCityERD" -> Parquet2DF(HOSP_ADDRESS_CITY_LOCATION),
            "hospProvinceERD" -> Parquet2DF(HOSP_ADDRESS_PROVINCE_LOCATION)
        )
    )("hospDIS")
    val prodDIS: DataFrame = prodCvs.toDIS(
        Map(
            "prodBaseERD" -> Parquet2DF(PROD_BASE_LOCATION),
            "prodDeliveryERD" -> Parquet2DF(PROD_DELIVERY_LOCATION),
            "prodDosageERD" -> Parquet2DF(PROD_DOSAGE_LOCATION),
            "prodMoleERD" -> Parquet2DF(PROD_MOLE_LOCATION),
            "prodPackageERD" -> Parquet2DF(PROD_PACKAGE_LOCATION),
            "prodCorpERD" -> Parquet2DF(PROD_CORP_LOCATION)
        )
    )("prodDIS")

    val maxDIS: DataFrame = pfizerInfMaxCvs.toDIS(
        Map(
            "maxERD" -> Parquet2DF(maxResultERDLocation),
            "hospDIS" -> hospDIS,
            "prodDIS" -> prodDIS
        )
    )("maxDIS")

    override def perform(pr: pActionArgs): pActionArgs = {
        phDebugLog("聚合开始:" + maxResultERDLocation)
        val marketDF = maxDIS.select("COMPANY_ID", "Province", "City", "MIN_PRODUCT", "TIME",  "SALES", "UNITS")
                .filter(col("COMPANY_ID") === companyId && col("TIME") >= ym.min && col("TIME") <= ym.max)
                .groupBy("MARKET", "TIME")
                .agg(expr("count(Province) as PROVINCE_COUNT"),
                    expr("count(City) as CITY_COUNT"),
                    expr("count(MIN_PRODUCT) as PRODUCT_COUNT"),
                    expr("sum(SALES) as SALES"),
                    expr("sum(UNITS) as UNITS"))

        phDebugLog("marketDF完成")

        val productDF = maxDIS.select("COMPANY_ID", "Province", "City", "MIN_PRODUCT", "TIME",  "SALES", "UNITS")
                .filter(col("COMPANY_ID") === companyId
                        && col("TIME") >= ym.min
                        && col("TIME") <= ym.max
//                        && col("MARKET") === market
                )
                .groupBy("MIN_PRODUCT", "TIME", "MARKET")
                .agg(expr("count(Province) as PROVINCE_COUNT"),
                    expr("count(City) as CITY_COUNT"),
                    expr("sum(SALES) as SALES"),
                    expr("sum(UNITS) as UNITS"))

        phDebugLog("productDF完成")

        super.perform(MapArgs(Map("productDF" -> DFArgs(marketDF), "productDF" -> DFArgs(productDF))))
    }
}
