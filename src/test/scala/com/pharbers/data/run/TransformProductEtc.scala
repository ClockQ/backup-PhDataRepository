package com.pharbers.data.run

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}
import com.pharbers.util.log.phLogTrait.phDebugLog

object TransformProductEtc extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    val productDevERD = Parquet2DF(PROD_DEV_LOCATION)
    val atcDF = Parquet2DF(PROD_ATCTABLE_LOCATION)

    val prodCvs = ProductEtcConversion()

    def nhwaProdEtcDF(): Unit = {
        val nhwa_company_id = NHWA_COMPANY_ID

        val nhwa_cpa_csv = "/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv"
        val nhwa_prod_match = "/data/nhwa/pha_config_repository1809/Nhwa_ProductMatchTable_20181126.csv"

        val cpaDF = CSV2DF(nhwa_cpa_csv).trim("COMPANY_ID", lit(nhwa_company_id)).trim("SOURCE", "CPA")
        val sourceDF = cpaDF
        val procMatchDF = CSV2DF(nhwa_prod_match).withColumnRenamed("PACK_COUNT", "PACK_NUMBER")
        val marketDF = try{
            Parquet2DF(PROD_MARKET_LOCATION + "/" + nhwa_company_id)
        } catch {
            case _: Exception => Seq.empty[(String, String, String)].toDF("_id", "PRODUCT_ID", "MARKET")
        }

        val productEtcERD = prodCvs.toERD(MapArgs(Map(
                "sourceDataDF" -> DFArgs(sourceDF)
            ))).getAs[DFArgs]("productEtcERD")
//        productEtcERD.show(false)

        if (args.isEmpty || args(0) == "TRUE") {
            productEtcERD.save2Mongo(PROD_ETC_LOCATION.split("/").last)
            productEtcERD.save2Parquet(PROD_ETC_LOCATION + "/" + nhwa_company_id)
        }

        val productEtcDIS = prodCvs.toDIS(MapArgs(Map(
            "productEtcERD" -> DFArgs(productEtcERD) //DFArgs(Parquet2DF(PROD_ETC_LOCATION + "/" + nhwa_company_id))
            , "atcERD" -> DFArgs(atcDF)
            , "marketERD" -> DFArgs(marketDF)
            , "productDevERD" -> DFArgs(productDevERD)
            , "productMatchDF" -> DFArgs(procMatchDF)
        ))).getAs[DFArgs]("productEtcDIS")
        productEtcDIS.show(false)

        // 未匹配到标准形式的只存在公司维度，max计算中是不计这部分的。
        val devNotMatch = productEtcDIS.filter(col("DEV_PRODUCT_ID").isNull)
        phDebugLog("nhwaProdNotMatch count = " + devNotMatch.count())  //0
        devNotMatch.show(false)

        val marketNotMatch = productEtcDIS.filter(col("MARKET").isNull)
        phDebugLog("nhwa product is null MARKET count:" + marketNotMatch.count())
        marketNotMatch.show(false)

        val atcNotMatch = productEtcDIS.filter(col("ATC_CODE").isNull)
        phDebugLog("nhwa product is null ATC_CODE count:" + atcNotMatch.count())
        atcNotMatch.show(false)
    }
    nhwaProdEtcDF()

    def pfizerProdEtcDF(): Unit = {
        val pfizer_company_id = PFIZER_COMPANY_ID

        val pfizer_cpa_csv = "/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv"
        val pfizer_gycx_csv = "/test/CPA&GYCX/Pfizer_201804_Gycx_20181127.csv"
        val pfizer_prod_match = "/data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv"

        val cpaDF = CSV2DF(pfizer_cpa_csv).trim("COMPANY_ID", lit(pfizer_company_id)).trim("SOURCE", "CPA")
        val gycDF = CSV2DF(pfizer_gycx_csv).trim("COMPANY_ID", lit(pfizer_company_id)).trim("SOURCE", "GYCX")
        val sourceDF = cpaDF unionByName gycDF
        val procMatchDF = CSV2DF(pfizer_prod_match)

        val marketDF = try{
            Parquet2DF(PROD_MARKET_LOCATION + "/" + pfizer_company_id)
        } catch {
            case _: Exception => Seq.empty[(String, String, String)].toDF("_id", "PRODUCT_ID", "MARKET")
        }

        val productEtcERD = prodCvs.toERD(MapArgs(Map(
                "sourceDataDF" -> DFArgs(sourceDF)
            ))).getAs[DFArgs]("productEtcERD")
//        productEtcERD.show(false)

        if (args.isEmpty || args(0) == "TRUE") {
            productEtcERD.save2Mongo(PROD_ETC_LOCATION.split("/").last)
            productEtcERD.save2Parquet(PROD_ETC_LOCATION + "/" + pfizer_company_id)
        }

        val productEtcDIS = prodCvs.toDIS(MapArgs(Map(
            "productEtcERD" -> DFArgs(Parquet2DF(PROD_ETC_LOCATION + "/" + pfizer_company_id))
            , "atcERD" -> DFArgs(atcDF)
            , "marketERD" -> DFArgs(marketDF)
            , "productDevERD" -> DFArgs(productDevERD)
            , "productMatchDF" -> DFArgs(procMatchDF)
        ))).getAs[DFArgs]("productEtcDIS")
//        productEtcDIS.show(false)

        phDebugLog("pfizer:", productEtcERD.count(), productEtcDIS.count())
        // 未匹配到标准形式的只存在公司维度，max计算中是不计这部分的。
        val devNotMatch = productEtcDIS.filter(col("DEV_PRODUCT_ID").isNull)
        phDebugLog("pfizer ProdNotMatch count = " + devNotMatch.count())  //0
        devNotMatch.show(false)

        val marketNotMatch = productEtcDIS.filter(col("MARKET").isNull)
        phDebugLog(" product is null MARKET count:" + marketNotMatch.count())
        marketNotMatch.show(false)

        val atcNotMatch = productEtcDIS.filter(col("ATC_CODE").isNull)
        phDebugLog("nhwa product is null ATC_CODE count:" + atcNotMatch.count())
        atcNotMatch.show(false)
    }
    pfizerProdEtcDF()

}