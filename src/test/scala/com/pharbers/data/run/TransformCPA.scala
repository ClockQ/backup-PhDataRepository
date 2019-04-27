package com.pharbers.data.run

import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs}

object TransformCPA extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    val hospCvs = HospConversion()
    val prodCvs = ProductEtcConversion()
    val cpaCvs = CPAConversion()

    val phaDF = Parquet2DF(HOSP_PHA_LOCATION)
    val phaDFCount = phaDF.count()
    val atcDF = Parquet2DF(PROD_ATCTABLE_LOCATION)
    val productDevERD = Parquet2DF(PROD_DEV_LOCATION)

    val hospDIS = hospCvs.toDIS(MapArgs(Map(
        "hospBaseERD" -> DFArgs(Parquet2DF(HOSP_BASE_LOCATION))
        , "hospAddressERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_BASE_LOCATION))
        , "hospPrefectureERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_PREFECTURE_LOCATION))
        , "hospCityERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_CITY_LOCATION))
        , "hospProvinceERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_PROVINCE_LOCATION))
    ))).getAs[DFArgs]("hospDIS")
    val hospDISCount = hospDIS.count()

    def nhwaCpaERD(): Unit = {
        val company_id = NHWA_COMPANY_ID

        val nhwa_cpa_csv = "/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv"
        val nhwa_prod_match = "/data/nhwa/pha_config_repository1809/Nhwa_ProductMatchTable_20181126.csv"

        val cpaDF = CSV2DF(nhwa_cpa_csv)
        val cpaDFCount = cpaDF.count()

        val marketDF = try{
            Parquet2DF(PROD_MARKET_LOCATION + "/" + company_id)
        } catch {
            case _: Exception => Seq.empty[(String, String, String)].toDF("_id", "PRODUCT_ID", "MARKET")
        }

        val prodMatchDF = CSV2DF(nhwa_prod_match)
                .trim("PACK_NUMBER").trim("PACK_COUNT")
                .withColumn("PACK_NUMBER", when($"PACK_NUMBER".isNotNull, $"PACK_NUMBER").otherwise($"PACK_COUNT"))

        val productEtcDIS = prodCvs.toDIS(MapArgs(Map(
            "productEtcERD" -> DFArgs(Parquet2DF(PROD_ETC_LOCATION + "/" + company_id))
            , "atcERD" -> DFArgs(atcDF)
            , "marketERD" -> DFArgs(marketDF)
            , "productDevERD" -> DFArgs(productDevERD)
            , "productMatchDF" -> DFArgs(prodMatchDF)
        ))).getAs[DFArgs]("productEtcDIS")
        val productEtcDISCount = productEtcDIS.count()

        val result = cpaCvs.toERD(MapArgs(Map(
            "cpaDF" -> DFArgs(cpaDF.trim("COMPANY_ID", company_id).trim("SOURCE", "CPA"))
            , "hospDF" -> DFArgs(hospDIS)
            , "prodDF" -> DFArgs(productEtcDIS)
            , "phaDF" -> DFArgs(phaDF)
            , "appendProdFunc" -> SingleArgFuncArgs { args: MapArgs =>
                prodCvs.toDIS(prodCvs.toERD(args))
            }
        )))

        val cpaERD = result.getAs[DFArgs]("cpaERD")
        val cpaERDCount = cpaERD.count()
        val cpaERDMinus = cpaDFCount - cpaERDCount
        assert(cpaERDMinus == 0, "nhwa: 转换后的ERD比源数据减少`" + cpaERDMinus + "`条记录")

        if(args.nonEmpty && args(0) == "TRUE"){
            cpaDF.save2Parquet(CPA_LOCATION + "/" + company_id)
            cpaDF.save2Mongo(CPA_LOCATION.split("/").last)
        }

        val cpaProd = result.getAs[DFArgs]("prodDIS")
        val cpaHosp = result.getAs[DFArgs]("hospDIS")
        val cpaPha = result.getAs[DFArgs]("phaDIS")
        phDebugLog("nhwa cpa ERD", cpaDFCount, cpaERDCount)
        phDebugLog("nhwa cpa Prod", productEtcDISCount, cpaProd.count())
        phDebugLog("nhwa cpa Hosp", hospDISCount, cpaHosp.count())
        phDebugLog("nhwa cpa Pha", phaDFCount, cpaPha.count())

        val cpaDIS = cpaCvs.toDIS(MapArgs(Map(
            "cpaERD" -> DFArgs(cpaERD)
            , "hospERD" -> DFArgs(cpaHosp)
            , "prodERD" -> DFArgs(cpaProd)
        ))).getAs[DFArgs]("cpaDIS")
        cpaDIS.show(false)
        val cpaDISMinus = cpaERDCount - cpaDIS.count()
        assert(cpaERDMinus == 0, "nhwa: 转换后的DIS比源数据减少`" + cpaDISMinus + "`条记录")
    }

    nhwaCpaERD()

    def pfizerCpaERD(): Unit = {
        val company_id = PFIZER_COMPANY_ID

        val pfizer_cpa_csv = "/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv"
        val pfizer_prod_match = "/data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv"

        val cpaDF = CSV2DF(pfizer_cpa_csv)
        val cpaDFCount = cpaDF.count()

        val marketDF = try{
            Parquet2DF(PROD_MARKET_LOCATION + "/" + company_id)
        } catch {
            case _: Exception => Seq.empty[(String, String, String)].toDF("_id", "PRODUCT_ID", "MARKET")
        }

        val prodMatchDF = CSV2DF(pfizer_prod_match)
                .trim("PACK_NUMBER").trim("PACK_COUNT")
                .withColumn("PACK_NUMBER", when($"PACK_NUMBER".isNotNull, $"PACK_NUMBER").otherwise($"PACK_COUNT"))

        val productEtcDIS = prodCvs.toDIS(MapArgs(Map(
            "productEtcERD" -> DFArgs(Parquet2DF(PROD_ETC_LOCATION + "/" + company_id))
            , "atcERD" -> DFArgs(atcDF)
            , "marketERD" -> DFArgs(marketDF)
            , "productDevERD" -> DFArgs(productDevERD)
            , "productMatchDF" -> DFArgs(prodMatchDF)
        ))).getAs[DFArgs]("productEtcDIS")
        val productEtcDISCount = productEtcDIS.count()

        val result = cpaCvs.toERD(MapArgs(Map(
            "cpaDF" -> DFArgs(cpaDF.trim("COMPANY_ID", company_id).trim("SOURCE", "CPA"))
            , "hospDF" -> DFArgs(hospDIS)
            , "prodDF" -> DFArgs(productEtcDIS)
            , "phaDF" -> DFArgs(phaDF)
            , "appendProdFunc" -> SingleArgFuncArgs { args: MapArgs =>
                prodCvs.toDIS(prodCvs.toERD(args))
            }
        )))

        val cpaERD = result.getAs[DFArgs]("cpaERD")
        val cpaERDCount = cpaERD.count()
        val cpaERDMinus = cpaDFCount - cpaERDCount
        assert(cpaERDMinus == 0, "pfizer: 转换后的ERD比源数据减少`" + cpaERDMinus + "`条记录")

        if(args.nonEmpty && args(0) == "TRUE"){
            cpaDF.save2Parquet(CPA_LOCATION + "/" + company_id)
            cpaDF.save2Mongo(CPA_LOCATION.split("/").last)
        }

        val cpaProd = result.getAs[DFArgs]("prodDIS")
        val cpaHosp = result.getAs[DFArgs]("hospDIS")
        val cpaPha = result.getAs[DFArgs]("phaDIS")
        phDebugLog("pfizer cpa ERD", cpaDFCount, cpaERDCount)
        phDebugLog("pfizer cpa Prod", productEtcDISCount, cpaProd.count())
        phDebugLog("pfizer cpa Hosp", hospDISCount, cpaHosp.count())
        phDebugLog("pfizer cpa Pha", phaDFCount, cpaPha.count())

        val cpaDIS = cpaCvs.toDIS(MapArgs(Map(
            "cpaERD" -> DFArgs(cpaERD)
            , "hospERD" -> DFArgs(cpaHosp)
            , "prodERD" -> DFArgs(cpaProd)
        ))).getAs[DFArgs]("cpaDIS")
        cpaDIS.show(false)
        val cpaDISMinus = cpaERDCount - cpaDIS.count()
        assert(cpaERDMinus == 0, "pfizer: 转换后的DIS比源数据减少`" + cpaDISMinus + "`条记录")
    }

//    pfizerCpaERD()
}