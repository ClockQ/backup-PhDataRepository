package com.pharbers.data.run

import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs}

object TransformGYCX extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    val hospCvs = HospConversion()
    val prodCvs = ProductEtcConversion()
    val gycxCvs = GYCXConversion()

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

    def pfizerGycxERD(): Unit = {
        val company_id = PFIZER_COMPANY_ID

        val pfizer_gycx_csv = "/test/CPA&GYCX/Pfizer_201804_Gycx_20181127.csv"
        val pfizer_prod_match = "/data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv"

        val gycxDF = CSV2DF(pfizer_gycx_csv)
        val gycxDFCount = gycxDF.count()

        val marketDF = {
            try{
                Parquet2DF(PROD_MARKET_LOCATION + "/" + company_id)
            } catch {
                case _: Exception => Seq.empty[(String, String, String)].toDF("_id", "PRODUCT_ID", "MARKET")
            }
        }
        val prodMatchDF = {
            CSV2DF(pfizer_prod_match)
                    .addColumn("PACK_NUMBER").addColumn("PACK_COUNT")
                    .withColumn("PACK_NUMBER", when($"PACK_NUMBER".isNotNull, $"PACK_NUMBER").otherwise($"PACK_COUNT"))
        }
        val productEtcDIS = prodCvs.toDIS(MapArgs(Map(
            "productEtcERD" -> DFArgs(Parquet2DF(PROD_ETC_LOCATION + "/" + company_id))
            , "atcERD" -> DFArgs(atcDF)
            , "marketERD" -> DFArgs(marketDF)
            , "productDevERD" -> DFArgs(productDevERD)
            , "productMatchDF" -> DFArgs(prodMatchDF)
        ))).getAs[DFArgs]("productEtcDIS")
        val productEtcDISCount = productEtcDIS.count()

        val result = gycxCvs.toERD(MapArgs(Map(
            "gycxDF" -> DFArgs(gycxDF.addColumn("COMPANY_ID", company_id).addColumn("SOURCE", "GYCX"))
            , "hospDF" -> DFArgs(hospDIS)
            , "prodDF" -> DFArgs(productEtcDIS)
            , "phaDF" -> DFArgs(phaDF)
            , "appendProdFunc" -> SingleArgFuncArgs { args: MapArgs =>
                prodCvs.toDIS(prodCvs.toERD(args))
            }
        )))

        val gycxERD = result.getAs[DFArgs]("gycxERD")
        val gycxERDCount = gycxERD.count()
        val gycxERDMinus = gycxDFCount - gycxERDCount
        assert(gycxERDMinus == 0, "pfizer: 转换后的ERD比源数据减少`" + gycxERDMinus + "`条记录")

        if(args.nonEmpty && args(0) == "TRUE"){
            gycxDF.save2Parquet(GYCX_LOCATION + "/" + company_id)
            gycxDF.save2Mongo(GYCX_LOCATION.split("/").last)
        }

        val gycxProd = result.getAs[DFArgs]("prodDIS")
        val gycxHosp = result.getAs[DFArgs]("hospDIS")
        val gycxPha = result.getAs[DFArgs]("phaDIS")
        phDebugLog("pfizer gycx ERD", gycxDFCount, gycxERDCount)
        phDebugLog("pfizer gycx Prod", productEtcDISCount, gycxProd.count())
        phDebugLog("pfizer gycx Hosp", hospDISCount, gycxHosp.count())
        phDebugLog("pfizer gycx Pha", phaDFCount, gycxPha.count())

        val gycxDIS = gycxCvs.toDIS(MapArgs(Map(
            "gycxERD" -> DFArgs(gycxERD)
            , "hospERD" -> DFArgs(gycxHosp)
            , "prodERD" -> DFArgs(gycxProd)
        ))).getAs[DFArgs]("gycxDIS")
        gycxDIS.show(false)
        val gycxDISMinus = gycxERDCount - gycxDIS.count()
        assert(gycxERDMinus == 0, "pfizer: 转换后的DIS比源数据减少`" + gycxDISMinus + "`条记录")
    }

    pfizerGycxERD()
}
