package com.pharbers.data.run

import com.pharbers.data.run.TransformCPA.prodCvs
import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs, StringArgs}

object TransformProductEtc extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._

//    implicit val sparkDriver: phSparkDriver = getSparkDriver()
//    implicit val conn: spark_conn_instance = sparkDriver.conn_instance
    import sparkDriver.ss.implicits._

    lazy val productDevERD = Parquet2DF(PROD_DEV_LOCATION)
    lazy val atcDF = Parquet2DF(PROD_ATCTABLE_LOCATION)

    lazy val prodCvs = ProductEtcConversion()

    def nhwaProdEtcDF(): Unit = {
        lazy val company_id = NHWA_COMPANY_ID

        lazy val prod_match_file = "/data/nhwa/pha_config_repository1809/Nhwa_ProductMatchTable_20181126.csv"
        lazy val market_match_file = "/data/nhwa/pha_config_repository1809/Nhwa_MarketMatchTable_20180629.csv"

        lazy val prodMatchDF = CSV2DF(prod_match_file)
                .addColumn("PACK_NUMBER").addColumn("PACK_COUNT")
                .withColumn("PACK_COUNT", when($"PACK_NUMBER".isNotNull, $"PACK_NUMBER").otherwise($"PACK_COUNT"))
        lazy val marketMatchDF = CSV2DF(market_match_file)

        lazy val productEtcERD = prodCvs.file2ERD(MapArgs(Map(
            "company_id" -> StringArgs(company_id)
            , "prodMatchDF" -> DFArgs(prodMatchDF)
            , "marketMatchDF" -> DFArgs(marketMatchDF)
            , "prodDevDF" -> DFArgs(productDevERD)
            , "matchMarketFunc" -> SingleArgFuncArgs { args: MapArgs =>
                val prodMatchDF = args.getAs[DFArgs]("prodMatchDF")
                val marketMatchDF = args.getAs[DFArgs]("marketMatchDF").select("MOLE_NAME", "MARKET")
                val resultDF = prodMatchDF.join(marketMatchDF, marketMatchDF("MOLE_NAME") === prodMatchDF("STANDARD_MOLE_NAME"))
                if(resultDF.filter($"MARKET".isNull).count() != 0)
                    throw new Exception("product exist null `MARKET`")
                MapArgs(Map("result" -> DFArgs(resultDF)))
            }
            , "matchDevFunc" -> SingleArgFuncArgs { args: MapArgs =>
                val prodMatchDF = args.getAs[DFArgs]("prodMatchDF")
                val prodDevDF = args.getAs[DFArgs]("prodDevDF").withColumn("DEV_PRODUCT_ID", $"_id")
                val resultDF = prodMatchDF.join(
                    prodDevDF
                    , prodMatchDF("STANDARD_PRODUCT_NAME") === prodDevDF("DEV_PRODUCT_NAME")
                            && prodMatchDF("STANDARD_MOLE_NAME") === prodDevDF("DEV_MOLE_NAME")
                            && prodMatchDF("STANDARD_DOSAGE") === prodDevDF("DEV_DOSAGE_NAME")
                            && prodMatchDF("STANDARD_PACK_DES") === prodDevDF("DEV_PACKAGE_DES")
                            && prodMatchDF("PACK_COUNT") === prodDevDF("DEV_PACKAGE_NUMBER")
                            && prodMatchDF("STANDARD_CORP_NAME") === prodDevDF("DEV_CORP_NAME")
                    , "left"
                )
                if(resultDF.filter($"DEV_PRODUCT_ID".isNull).count() != 0)
                    throw new Exception("product exist null `DEV_PRODUCT_ID`")
                MapArgs(Map("result" -> DFArgs(resultDF)))
            }
        ))).getAs[DFArgs]("productEtcERD")
        lazy val productEtcERDCount = productEtcERD.count()
//        productEtcERD.show(false)

        if(args.nonEmpty && args(0) == "TRUE")
            productEtcERD.save2Parquet(PROD_ETC_LOCATION + "/" + company_id).save2Mongo(PROD_ETC_LOCATION.split("/").last)

        lazy val productEtcDIS = prodCvs.mergeERD(MapArgs(Map(
            "productEtcERD" -> DFArgs(productEtcERD) //DFArgs(Parquet2DF(PROD_ETC_LOCATION + "/" + company_id))
            , "atcERD" -> DFArgs(atcDF)
            , "productDevERD" -> DFArgs(productDevERD)
        ))).getAs[DFArgs]("productEtcDIS")
        lazy val productEtcDISCount = productEtcDIS.count()
//        productEtcDIS.show(false)

        // 未匹配到标准形式的只存在公司维度，max计算中是不计这部分的。
        val devNotMatch = productEtcDIS.filter(col("DEV_PRODUCT_ID").isNull)  //0
        phDebugLog("nhwa ProdNotMatch count = " + devNotMatch.count())
        devNotMatch.show(false)

        val marketNotMatch = productEtcDIS.filter(col("MARKET").isNull)
        phDebugLog("nhwa product is null MARKET count:" + marketNotMatch.count())  //0
        marketNotMatch.show(false)

        val atcNotMatch = productEtcDIS.filter(col("ATC_CODE").isNull)
        phDebugLog("nhwa product is null ATC_CODE count:" + atcNotMatch.count())  //0
        atcNotMatch.show(false)

        if(args.nonEmpty && args(0) == "TRUE")
            productEtcDIS.save2Parquet(PROD_ETC_DIS_LOCATION + "/" + company_id)
    }
    nhwaProdEtcDF()

//    def pfizerProdEtcDF(): Unit = {
//        val company_id = PFIZER_COMPANY_ID
//
//        val pfizer_cpa_csv = "/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv"
//        val pfizer_gycx_csv = "/test/CPA&GYCX/Pfizer_201804_Gycx_20181127.csv"
//        val pfizer_prod_match = "/data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv"
//
//        val cpaDF = CSV2DF(pfizer_cpa_csv).addColumn("COMPANY_ID", lit(company_id)).addColumn("SOURCE", "CPA")
//        val gycDF = CSV2DF(pfizer_gycx_csv).addColumn("COMPANY_ID", lit(company_id)).addColumn("SOURCE", "GYCX")
//        val sourceDF = cpaDF unionByName gycDF
//        val prodMatchDF = CSV2DF(pfizer_prod_match)
//                .addColumn("PACK_NUMBER").addColumn("PACK_COUNT")
//                .withColumn("PACK_NUMBER", when($"PACK_NUMBER".isNotNull, $"PACK_NUMBER").otherwise($"PACK_COUNT"))
//
//        val marketDF = try{
//            Parquet2DF(PROD_MARKET_LOCATION + "/" + company_id)
//        } catch {
//            case _: Exception => Seq.empty[(String, String, String)].toDF("_id", "PRODUCT_ID", "MARKET")
//        }
//
//        val productEtcERD = prodCvs.toERD(MapArgs(Map(
//                "sourceDataDF" -> DFArgs(sourceDF)
//            ))).getAs[DFArgs]("productEtcERD")
//        productEtcERD.show(false)
//
//        if(args.nonEmpty && args(0) == "TRUE")
//            productEtcERD.save2Mongo(PROD_ETC_LOCATION.split("/").last).save2Parquet(PROD_ETC_LOCATION + "/" + company_id)
//
//        val productEtcDIS = prodCvs.toDIS(MapArgs(Map(
//            "productEtcERD" -> DFArgs(Parquet2DF(PROD_ETC_LOCATION + "/" + company_id))
//            , "atcERD" -> DFArgs(atcDF)
//            , "marketERD" -> DFArgs(marketDF)
//            , "productDevERD" -> DFArgs(productDevERD)
//            , "productMatchDF" -> DFArgs(prodMatchDF)
//        ))).getAs[DFArgs]("productEtcDIS")
//        productEtcDIS.show(false)
//
//        phDebugLog("pfizer:", productEtcERD.count(), productEtcDIS.count())
//        // 未匹配到标准形式的只存在公司维度，max计算中是不计这部分的。
//        val devNotMatch = productEtcDIS.filter(col("DEV_PRODUCT_ID").isNull)
//        phDebugLog("pfizer ProdNotMatch count = " + devNotMatch.count())  //0
//        devNotMatch.show(false)
//
//        val marketNotMatch = productEtcDIS.filter(col("MARKET").isNull)
//        phDebugLog("pfizer product is null MARKET count:" + marketNotMatch.count())
//        marketNotMatch.show(false)
//
//        val atcNotMatch = productEtcDIS.filter(col("ATC_CODE").isNull)
//        phDebugLog("pfizer product is null ATC_CODE count:" + atcNotMatch.count())
//        atcNotMatch.show(false)
//    }
//    pfizerProdEtcDF()

}