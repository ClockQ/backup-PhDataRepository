package com.pharbers.data.agg

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.pharbers.data.util.ParquetLocation._
import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs}

/**
  * @description:
  * @author: clock
  * @date: 2019-04-19 13:07
  */
object AggMain extends App {
//    val aggFactory = new Agg(NHWA_COMPANY_ID)
//    aggFactory.setProdMatch("/data/nhwa/pha_config_repository1809/Nhwa_ProductMatchTable_20181126.csv")
//    aggFactory.setSource("/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv")
    val aggFactory = new Agg(PFIZER_COMPANY_ID)
    aggFactory.setProdMatch("/data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv")
    aggFactory.setSource("/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv", "/test/CPA&GYCX/Pfizer_201804_Gycx_20181127.csv")

    phDebugLog("Market")
    val market = aggFactory.getMarket
    println("market count = " + market.length)
//    market.foreach(println)

    phDebugLog("Mole")
    val mole = aggFactory.getMole
    println("mole count = " + mole.length)
//    mole.foreach(println)

    phDebugLog("Product Name")
    val product = aggFactory.getProductName
    println("product count = " + product.length)
//    product.foreach(println)

    phDebugLog("Provinces")
    val provinces = aggFactory.getProvinces
    println("provinces count = " + provinces.length)

    phDebugLog("City")
    val city = aggFactory.getCity
    println("city count = " + city.length)

    phDebugLog("Hospital")
    val hospital = aggFactory.getHosp
    println("hospital count = " + hospital.length)
//    hospital.foreach(println)

    phDebugLog("countByTime")
    val countByTime = aggFactory.getCountByTime
    countByTime.foreach(println)

    phDebugLog("countByProvince")
    val countByProvince = aggFactory.getCountByProvince
    countByProvince.foreach(println)

    phDebugLog("countByHospType")
    val countByHospType = aggFactory.getCountByHospType
    countByHospType.foreach(println)

}

class Agg(company_id: String) {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    var sourceDF: DataFrame = Seq.empty[String].toDF("_id")
    var prodMatchDF: DataFrame = Seq.empty[String].toDF("_id")

    val productEtcERD = Parquet2DF(PROD_ETC_LOCATION + "/" + company_id)
    val atcDF = Parquet2DF(PROD_ATCTABLE_LOCATION)
    val productDevERD = Parquet2DF(PROD_DEV_LOCATION)
    val phaDF = Parquet2DF(HOSP_PHA_LOCATION)

    def setProdMatch(prod_match: String): Agg = {
        val matchDF = CSV2DF(prod_match)
                .addColumn("PACK_NUMBER").addColumn("PACK_COUNT")
                .withColumn("PACK_NUMBER", when($"PACK_NUMBER".isNotNull, $"PACK_NUMBER").otherwise($"PACK_COUNT"))

        prodMatchDF = prodMatchDF.alignAt(matchDF).unionByName(matchDF.alignAt(prodMatchDF))

        this
    }

    def setSource(cpa_file: String = "", gycx_file: String = ""): Agg = {
        if (cpa_file.nonEmpty) {
            val cpaDF = CSV2DF(cpa_file).withColumn("COMPANY_ID", lit(company_id))
            val cpaERD = CPAConversion().toERD(MapArgs(Map(
                "cpaDF" -> DFArgs(cpaDF)
                , "hospDF" -> DFArgs(hospDIS)
                , "prodDF" -> DFArgs(prodDIS)
                , "phaDF" -> DFArgs(phaDF)
            ))).getAs[DFArgs]("cpaERD")
            sourceDF = sourceDF.alignAt(cpaERD).unionByName(cpaERD.alignAt(sourceDF))
        }

        if (gycx_file.nonEmpty) {
            val gycxDF = CSV2DF(gycx_file).withColumn("COMPANY_ID", lit(company_id))
            val gycxERD = GYCXConversion().toERD(MapArgs(Map(
                "gycxDF" -> DFArgs(gycxDF)
                , "hospDF" -> DFArgs(hospDIS)
                , "prodDF" -> DFArgs(prodDIS)
                , "phaDF" -> DFArgs(phaDF)
            ))).getAs[DFArgs]("gycxERD")
            sourceDF = sourceDF.alignAt(gycxERD).unionByName(gycxERD.alignAt(sourceDF))
        }

        this
    }

    lazy val marketDF = Parquet2DF(PROD_MARKET_LOCATION + "/" + company_id)

    lazy val prodDIS: DataFrame = {
        val productEtcDIS = ProductEtcConversion2().toDIS(MapArgs(Map(
            "productEtcERD" -> DFArgs(productEtcERD)
            , "atcERD" -> DFArgs(atcDF)
            , "marketERD" -> DFArgs(marketDF)
            , "productDevERD" -> DFArgs(productDevERD)
            , "productMatchDF" -> DFArgs(prodMatchDF)
        ))).getAs[DFArgs]("productEtcDIS")

        productEtcDIS
    }

    lazy val hospDIS: DataFrame = {
        HospConversion().toDIS(MapArgs(Map(
            "hospBaseERD" -> DFArgs(Parquet2DF(HOSP_BASE_LOCATION))
            , "hospAddressERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_BASE_LOCATION))
            , "hospPrefectureERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_PREFECTURE_LOCATION))
            , "hospCityERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_CITY_LOCATION))
            , "hospProvinceERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_PROVINCE_LOCATION))
        ))).getAs[DFArgs]("hospDIS")
    }

    lazy val sourceHosp: DataFrame = {
        sourceDF
                .join(
                    hospDIS
                    , sourceDF("HOSPITAL_ID") === hospDIS("_id")
                    , "left"
                )
                .withColumn("PROVINCE_NAME",
                    when(col("PROVINCE_NAME").isNull, lit("other"))
                            .otherwise(col("PROVINCE_NAME"))
                )
                .withColumn("CITY_NAME",
                    when(col("CITY_NAME").isNull, lit("other"))
                            .otherwise(col("CITY_NAME"))
                )
                .withColumn("HOSP_NAME",
                    when(col("HOSP_NAME").isNull, lit("other"))
                            .otherwise(col("HOSP_NAME"))
                )
                .withColumn("HOSP_TYPE",
                    when(col("HOSP_TYPE").isNull, lit("other"))
                            .otherwise(col("HOSP_TYPE"))
                )
    }

    def getMarket: List[(String, String)] = marketDF.groupBy("MARKET").count().collect()
            .map(x => x.getAs[String](0) -> x.getAs[String](1))
            .toList

    def getMole: List[(String, String)] = prodDIS.groupBy("ETC_MOLE_NAME").count().collect()
            .map(x => x.getAs[String](0) -> x.getAs[String](1))
            .toList

    def getProductName: Array[String] = prodDIS.select("ETC_PRODUCT_NAME").distinct().collect().map(_.getAs[String](0))

    def getProvinces: Array[String] = sourceHosp.select("PROVINCE_NAME").distinct().collect().map(_.getAs[String](0))

    def getCity: Array[String] = sourceHosp.select("CITY_NAME").distinct().collect().map(_.getAs[String](0))

    def getHosp: Array[String] = sourceHosp.select("HOSPITAL_ID").distinct().collect().map(_.getAs[Int](0).toString)

    def getCountByTime: List[(String, String)] = sourceDF.groupBy("YM").count().sort(col("YM").asc).collect()
            .map(x => x.getAs[String](0) -> x.getAs[String](1))
            .toList

    def getCountByProvince: List[(String, Long)] = sourceHosp.groupBy("PROVINCE_NAME").count().sort(col("count").desc).collect()
            .map(x => x.getAs[String](0) -> x.getAs[Long](1))
            .toList

    def getCountByHospType: List[(String, Long, Double)] = {
        val hospDF = sourceHosp.select("HOSPITAL_ID", "HOSP_TYPE").distinct()
        val hospCount = hospDF.count()
        val hospTypeCount = hospDF.groupBy("HOSP_TYPE").count().sort(col("count").desc).collect()
                .map(x => (
                        x.getAs[String](0)
                        , x.getAs[Long](1)
                        , (x.getAs[Long](1) / hospCount.toDouble).formatted("%1.5f").toDouble
                        )
                )
                .toList
        ("全部", hospCount, 1.0) :: hospTypeCount
    }

//    def getSampleHosp(): Unit = {
//        val a = CSV2DF("/test/2019年Universe更新维护1.0.csv")
//        a.show(false)
//        sourceDF.show(false)
//    }

}

