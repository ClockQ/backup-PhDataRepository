package com.pharbers.data.run

import com.pharbers.util.log.phLogTrait.phDebugLog

object TransformProductEtc extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._

    val productDevERD = Parquet2DF(PROD_DEV_LOCATION)
    val atcParquet = Parquet2DF(PROD_ATCTABLE_LOCATION)

    val prodCvs = ProductEtcConversion()

    def nhwaProdEtcDF(): Unit = {
        val nhwa_company_id = NHWA_COMPANY_ID

        val nhwa_cpa_csv = "/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv"
        val nhwa_prod_match = "/data/nhwa/pha_config_repository1809/Nhwa_ProductMatchTable_20181126.csv"

        val nhwaCpaDF = CSV2DF(nhwa_cpa_csv).trim("COMPANY_ID", lit(nhwa_company_id)).trim("SOURCE", "CPA")
        val nhwaProcMatch = CSV2DF(nhwa_prod_match).withColumnRenamed("PACK_COUNT", "PACK_NUMBER")

        val nhwaProductEtcERD = prodCvs.toERD(
            Map(
                "sourceDataDF" -> nhwaCpaDF,
                "productDevERD" -> productDevERD,
                "productMatchDF" -> nhwaProcMatch
            )
        )("productEtcERD")
        nhwaProductEtcERD.show(false)

        //TODO：未匹配到标准形式的只存在公司维度，max计算中是不计这部分的。
        val nhwaProdNotMatch = nhwaProductEtcERD.filter(col("PRODUCT_ID") === "")
        phDebugLog("nhwaProdNotMatch count = " + nhwaProdNotMatch.count())  //0

        if (args.isEmpty || args(0) == "TRUE") {
            nhwaProductEtcERD.save2Mongo(PROD_ETC_LOCATION.split("/").last)
            nhwaProductEtcERD.save2Parquet(PROD_ETC_LOCATION + "/" + nhwa_company_id)
        }

        val nhwaProdEtcParquet = Parquet2DF(PROD_ETC_LOCATION + "/" + nhwa_company_id)
        nhwaProdEtcParquet.show(false)
        phDebugLog("nhwa prod etc count:" + nhwaProdEtcParquet.count())
        phDebugLog("nhwa prod by duplicates count:" + nhwaProdEtcParquet.dropDuplicates("PRODUCT_ID").count())

        val nhwaMarketParquet = Parquet2DF(PROD_MARKET_LOCATION + "/" + nhwa_company_id)
        val nhwaProductEtcDIS = prodCvs.toDIS(Map(
            "productEtcERD" -> nhwaProdEtcParquet
            , "marketERD" -> nhwaMarketParquet
            , "atcERD" -> atcParquet
        ))("productEtcDIS")
        phDebugLog("nhwa product is null MARKET count:" + nhwaProductEtcDIS.filter(col("MARKET").isNull).count())
        phDebugLog("nhwa product is null ATC_CODE count:" + nhwaProductEtcDIS.filter(col("ATC_CODE").isNull).count())

    }
    nhwaProdEtcDF()

    def pfizerProdEtcDF(): Unit = {
        val pfizer_company_id = PFIZER_COMPANY_ID

        val pfizer_cpa_csv = "/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv"
        val pfizer_gycx_csv = "/test/CPA&GYCX/Pfizer_201804_Gycx_20181127.csv"
        val pfizer_prod_match = "/data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv"

        val pfizerCpaDF = CSV2DF(pfizer_cpa_csv).trim("COMPANY_ID", lit(pfizer_company_id)).trim("SOURCE", "CPA")
        val pfizerGycDF = CSV2DF(pfizer_gycx_csv).trim("COMPANY_ID", lit(pfizer_company_id)).trim("SOURCE", "GYCX")
        val pfizerSource = pfizerCpaDF unionByName pfizerGycDF
        val pfizerProcMatch = CSV2DF(pfizer_prod_match)

        val pfizerProductEtcERD = prodCvs.toERD(
            Map(
                "sourceDataDF" -> pfizerSource,
                "productDevERD" -> productDevERD,
                "productMatchDF" -> pfizerProcMatch
            )
        )("productEtcERD")
        pfizerProductEtcERD.show(false)

        //TODO：未匹配到标准形式的只存在公司维度，max计算中是不计这部分的。
        val pfizerProdNotMatch = pfizerProductEtcERD.filter(col("PRODUCT_ID") === "")
        phDebugLog("pfizerProdNotMatch count = " + pfizerProdNotMatch.count())  //935

        if (args.isEmpty || args(0) == "TRUE") {
            pfizerProductEtcERD.save2Mongo(PROD_ETC_LOCATION.split("/").last)
            pfizerProductEtcERD.save2Parquet(PROD_ETC_LOCATION + "/" + pfizer_company_id)
        }

        val pfizerProdEtcParquet = Parquet2DF(PROD_ETC_LOCATION + "/" + pfizer_company_id)
        pfizerProdEtcParquet.show(false)
        phDebugLog("pfizer prod etc count:" + pfizerProdEtcParquet.count())
        phDebugLog("pfizer prod by duplicates count:" + pfizerProdEtcParquet.dropDuplicates("PRODUCT_ID").count())

        val pfizerMarketParquet = Parquet2DF(PROD_MARKET_LOCATION + "/" + PFIZER_COMPANY_ID)
        val pfizerProductEtcDIS = prodCvs.toDIS(Map(
            "productEtcERD" -> pfizerProdEtcParquet
            , "marketERD" -> pfizerMarketParquet
            , "atcERD" -> atcParquet
        ))("productEtcDIS")
        phDebugLog("pfizer product is null MARKET count:" + pfizerProductEtcDIS.filter(col("MARKET").isNull).count())
        phDebugLog("pfizer product is null ATC_CODE count:" + pfizerProductEtcDIS.filter(col("ATC_CODE").isNull).count())
    }
    pfizerProdEtcDF()
}