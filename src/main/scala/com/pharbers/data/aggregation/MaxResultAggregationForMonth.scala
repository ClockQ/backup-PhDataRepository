package com.pharbers.data.aggregation

import com.pharbers.data.conversion.{HospConversion, MaxResultConversion, ProductDevConversion2}
import com.pharbers.data.util.ParquetLocation._
import com.pharbers.data.util._
import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.util.log.phLogTrait.phDebugLog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class MaxResultAggregationForMonth(args: MapArgs)(implicit any: Any = null) extends sequenceJobWithMap {
    override val actions: List[pActionTrait] = Nil
    override val name: String = "max result aggregation by month"

    val maxResultERDLocation: String = args.get("max_result_erd_location").getBy[StringArgs]
    val ym: Seq[Int] = args.get("ym").getBy[StringArgs].split(",").map(x => x.toInt)
    val companyId: String = args.get("company").getBy[StringArgs]
    val months: Seq[String] = args.get("months").getBy[StringArgs].split(",")
    val aggPath: String = args.get("aggPath").getBy[StringArgs] // 稳定后使用MAX_RESULT_ADDRESS_AGG_LOCATION
    val maxDIS: DataFrame = args.get.getOrElse("maxDIS", throw new Exception("not found maxDIS")).getBy[DFArgs]


    override def perform(pr: pActionArgs): pActionArgs = {
        phDebugLog("聚合开始:" + maxResultERDLocation)

        val maxMiddleDF = maxDIS
                .select(col("COMPANY_ID"), col("province-name"), col("city-name")
                    , col("MIN_PRODUCT"), col("YM"), col("SALES")
                    , col("UNITS"), col("MARKET"), col("PRODUCT_NAME")
                    , col("MOLE_NAME"), col("CORP_NAME"), col("PH_CORP_NAME"))
                .withColumnRenamed("province-name", "province")
                .withColumnRenamed("city-name", "city")
                .addMonth()
                .filter(col("COMPANY_ID") === companyId
                        && col("YM") >= ym.min
                        && col("YM") <= ym.max
                )

                months.foreach(x => {
                    val oneMonthAgg = maxMiddleDF
                            .filter(col("MONTH") === x)
                            .groupBy("MIN_PRODUCT", "YM") // 消除了地区维度
                            .agg(expr("sum(SALES) as SALES"),
                                expr("sum(UNITS) as UNITS"),
                                expr("first(COMPANY_ID) as COMPANY_ID"),
                                expr("first(PRODUCT_NAME) as PRODUCT_NAME"),
                                expr("first(MOLE_NAME) as MOLE_NAME"),
                                expr("first(CORP_NAME) as CORP_NAME"),
                                expr("first(PH_CORP_NAME) as PH_CORP_NAME"))
                            .withColumn("time", unix_timestamp)
                    phDebugLog(s"消除了地区,聚合$x 月 maxAggDF完成")
                    oneMonthAgg.save2Parquet(aggPath) // 稳定后使用MAX_RESULT_ADDRESS_AGG_LOCATION
                })

        MapArgs(Map(
            "result" -> StringArgs("Conversion success")
        ))
    }
}
