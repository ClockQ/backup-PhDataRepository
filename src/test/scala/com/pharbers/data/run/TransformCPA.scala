package com.pharbers.data.run

import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs, StringArgs}

object TransformCPA extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    val cpaCvs = CPAConversion()

    lazy val phaDF = Parquet2DF(HOSP_PHA_LOCATION)
    lazy val phaDFCount = phaDF.count()
    lazy val hospDIS = Parquet2DF(HOSP_DIS_LOCATION)
    lazy val hospDISCount = hospDIS.count()

    def nhwaCpaERD(): Unit = {
        val company_id = NHWA_COMPANY_ID

        val cpa_csv_file = "/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv"
        val prod_match_file = "/data/nhwa/pha_config_repository1809/Nhwa_ProductMatchTable_20181126.csv"

        lazy val cpaDF = CSV2DF(cpa_csv_file)
        lazy val cpaDFCount = cpaDF.count()

        lazy val prodMatchDF = CSV2DF(prod_match_file)
                .addColumn("PACK_NUMBER").addColumn("PACK_COUNT")
                .withColumn("PACK_NUMBER", when($"PACK_NUMBER".isNotNull, $"PACK_NUMBER").otherwise($"PACK_COUNT"))

        lazy val productEtcDIS = Parquet2DF(PROD_ETC_DIS_LOCATION + "/" + company_id)

        lazy val cpaERD = cpaCvs.toERD(MapArgs(Map(
            "company_id" -> StringArgs(company_id)
            , "source" -> StringArgs("CPA")
            , "cpaDF" -> DFArgs(cpaDF)
            , "hospDF" -> DFArgs(hospDIS)
            , "prodDF" -> DFArgs(productEtcDIS)
            , "phaDF" -> DFArgs(phaDF)
            , "prodMatchDF" -> DFArgs(prodMatchDF)
            , "matchHospFunc" -> SingleArgFuncArgs(cpaCvs.matchHospFunc)
            , "matchProdFunc" -> SingleArgFuncArgs(cpaCvs.matchProdFunc)
        ))).getAs[DFArgs]("cpaERD")
        lazy val cpaERDCount = cpaERD.count()
//        cpaERD.show(false)
        lazy val cpaERDMinus = cpaDFCount - cpaERDCount
        assert(cpaERDMinus == 0, "nhwa: 转换后的ERD比源数据减少`" + cpaERDMinus + "`条记录")

        if(args.nonEmpty && args(0) == "TRUE")
            cpaERD.save2Parquet(CPA_LOCATION + "/" + company_id + "/20181227")//.save2Mongo(CPA_LOCATION.split("/").last)

        lazy val cpaDIS = cpaCvs.toDIS(MapArgs(Map(
            "cpaERD" -> DFArgs(cpaERD) //DFArgs(Parquet2DF(CPA_LOCATION + "/" + company_id + "/20181227"))
            , "hospDIS" -> DFArgs(hospDIS)
            , "prodDIS" -> DFArgs(productEtcDIS)
        ))).getAs[DFArgs]("cpaDIS")
        lazy val cpaDISCount = cpaDIS.count()
//        cpaDIS.show(false)
        lazy val cpaDISMinus = cpaDFCount - cpaDISCount
        assert(cpaERDMinus == 0, "nhwa: 转换后的DIS比源数据减少`" + cpaDISMinus + "`条记录")
    }
    nhwaCpaERD()

//    def pfizerCpaERD(): Unit = {
//        val company_id = PFIZER_COMPANY_ID
//
//        val pfizer_cpa_csv = "/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv"
//        val pfizer_prod_match = "/data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv"
//
//        val cpaDF = CSV2DF(pfizer_cpa_csv)
//        val cpaDFCount = cpaDF.count()
//
//        val marketDF = try{
//            Parquet2DF(PROD_MARKET_LOCATION + "/" + company_id)
//        } catch {
//            case _: Exception => Seq.empty[(String, String, String)].toDF("_id", "PRODUCT_ID", "MARKET")
//        }
//
//        val prodMatchDF = CSV2DF(pfizer_prod_match)
//                .addColumn("PACK_NUMBER").addColumn("PACK_COUNT")
//                .withColumn("PACK_NUMBER", when($"PACK_NUMBER".isNotNull, $"PACK_NUMBER").otherwise($"PACK_COUNT"))
//
//        val productEtcDIS = prodCvs.toDIS(MapArgs(Map(
//            "productEtcERD" -> DFArgs(Parquet2DF(PROD_ETC_LOCATION + "/" + company_id))
//            , "atcERD" -> DFArgs(atcDF)
//            , "marketERD" -> DFArgs(marketDF)
//            , "productDevERD" -> DFArgs(productDevERD)
//            , "productMatchDF" -> DFArgs(prodMatchDF)
//        ))).getAs[DFArgs]("productEtcDIS")
//        val productEtcDISCount = productEtcDIS.count()
//
//        val result = cpaCvs.toERD(MapArgs(Map(
//            "cpaDF" -> DFArgs(cpaDF.addColumn("COMPANY_ID", company_id).addColumn("SOURCE", "CPA"))
//            , "hospDF" -> DFArgs(hospDIS)
//            , "prodDF" -> DFArgs(productEtcDIS)
//            , "phaDF" -> DFArgs(phaDF)
//            , "appendProdFunc" -> SingleArgFuncArgs { args: MapArgs =>
//                prodCvs.toDIS(prodCvs.toERD(args))
//            }
//        )))
//
//        val cpaERD = result.getAs[DFArgs]("cpaERD")
//        val cpaERDCount = cpaERD.count()
//        val cpaERDMinus = cpaDFCount - cpaERDCount
//        assert(cpaERDMinus == 0, "pfizer: 转换后的ERD比源数据减少`" + cpaERDMinus + "`条记录")
//
//        if(args.nonEmpty && args(0) == "TRUE"){
//            cpaDF.save2Parquet(CPA_LOCATION + "/" + company_id)
//            cpaDF.save2Mongo(CPA_LOCATION.split("/").last)
//        }
//
//        val cpaProd = result.getAs[DFArgs]("prodDIS")
//        val cpaHosp = result.getAs[DFArgs]("hospDIS")
//        val cpaPha = result.getAs[DFArgs]("phaDIS")
//        phDebugLog("pfizer cpa ERD", cpaDFCount, cpaERDCount)
//        phDebugLog("pfizer cpa Prod", productEtcDISCount, cpaProd.count())
//        phDebugLog("pfizer cpa Hosp", hospDISCount, cpaHosp.count())
//        phDebugLog("pfizer cpa Pha", phaDFCount, cpaPha.count())
//
//        val cpaDIS = cpaCvs.toDIS(MapArgs(Map(
//            "cpaERD" -> DFArgs(cpaERD)
//            , "hospERD" -> DFArgs(cpaHosp)
//            , "prodERD" -> DFArgs(cpaProd)
//        ))).getAs[DFArgs]("cpaDIS")
//        cpaDIS.show(false)
//        val cpaDISMinus = cpaERDCount - cpaDIS.count()
//        assert(cpaERDMinus == 0, "pfizer: 转换后的DIS比源数据减少`" + cpaDISMinus + "`条记录")
//    }

//    pfizerCpaERD()
}