package com.pharbers.data.run

import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.data.conversion.ProductEtcConversion
import com.pharbers.data.run.TransformGYCX.args
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

object TransformMarket extends App {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    val productDevERD = Parquet2DF(PROD_DEV_LOCATION)

    val prodCvs = ProductEtcConversion()

    def nhwaMarketDF(): Unit = {
        val nhwa_company_id = NHWA_COMPANY_ID

        val market_match_file = "/data/nhwa/pha_config_repository1809/Nhwa_MarketMatchTable_20180629.csv"
        val prod_match_file = "/data/nhwa/pha_config_repository1809/Nhwa_ProductMatchTable_20181126.csv"

        val marketTableDF = CSV2DF(market_match_file)
        val prodMatchDF = CSV2DF(prod_match_file)
                .trim("PACK_NUMBER").trim("PACK_COUNT")
                .withColumn("PACK_NUMBER", when($"PACK_NUMBER".isNotNull, $"PACK_NUMBER").otherwise($"PACK_COUNT"))

        val productEtcDIS = prodCvs
                .toDIS(MapArgs(Map(
                    "productEtcERD" -> DFArgs(Parquet2DF(PROD_ETC_LOCATION + "/" + nhwa_company_id))
                    , "productDevERD" -> DFArgs(productDevERD)
                    , "productMatchDF" -> DFArgs(prodMatchDF)
                )))
                .getAs[DFArgs]("productEtcDIS")
                .withColumn("MIN2",
                    concat(col("DEV_PRODUCT_NAME"), col("DEV_DOSAGE_NAME"), col("DEV_PACKAGE_DES"), col("DEV_PACKAGE_NUMBER"), col("DEV_CORP_NAME"))
                )

        val marketERD = productEtcDIS
                .join(
                    marketTableDF,
                    productEtcDIS("ETC_MOLE_NAME") === marketTableDF("MOLE_NAME"),
                    "left"
                )
                .select(productEtcDIS("DEV_PRODUCT_ID").as("PRODUCT_ID"), marketTableDF("MARKET"))
                .distinct()
                .generateId
        marketERD.show(false)

        if(args.nonEmpty && args(0) == "TRUE"){
            marketERD.save2Mongo(PROD_MARKET_LOCATION.split("/").last)
            marketERD.save2Parquet(PROD_MARKET_LOCATION + "/" + nhwa_company_id)
        }

        val marketParquet = Parquet2DF(PROD_MARKET_LOCATION + "/" + nhwa_company_id)
        phDebugLog("nhwa market:" + marketParquet.count())
        phDebugLog("nhwa market by duplicates:" + marketParquet.dropDuplicates("PRODUCT_ID").count())
    }

    nhwaMarketDF()

    def phizerMarketDF(): Unit = {
        val pfizer_company_id = PFIZER_COMPANY_ID

        val prod_match_file = "/data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv"

        val pfizer_AI_D_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-AI_D.csv"
        val pfizer_AI_R_other_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-AI_R_other.csv"
        val pfizer_AI_R_zith_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-AI_R_zith.csv"
        val pfizer_AI_S_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-AI_S.csv"
        val pfizer_AI_W_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-AI_W.csv"
        val pfizer_CNS_R_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-CNS_R.csv"
        val pfizer_CNS_Z_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-CNS_Z.csv"
        val pfizer_DVP_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-DVP.csv"
        val pfizer_ELIQUIS_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-ELIQUIS.csv"
        val pfizer_HTN_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-HTN.csv"
        val pfizer_HTN2_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-HTN2.csv"
        val pfizer_INF_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-INF.csv"
        val pfizer_LD_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-LD.csv"
        val pfizer_ONC_aml_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-ONC_aml.csv"
        val pfizer_ONC_other_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-ONC_other.csv"
        val pfizer_PAIN_C_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-PAIN_C.csv"
        val pfizer_PAIN_lyrica_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-PAIN_lyrica.csv"
        val pfizer_PAIN_other_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-PAIN_other.csv"
        val pfizer_Specialty_champix_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-Specialty_champix.csv"
        val pfizer_Specialty_other_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-Specialty_other.csv"
        val pfizer_Urology_other_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-Urology_other.csv"
        val pfizer_Urology_viagra_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-Urology_viagra.csv"
        val pfizer_ZYVOX_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-ZYVOX.csv"

        val maxResultLst =
            pfizer_AI_D_csv :: pfizer_AI_R_other_csv :: pfizer_AI_R_zith_csv :: pfizer_AI_S_csv ::
                    pfizer_AI_W_csv :: pfizer_CNS_R_csv :: pfizer_CNS_Z_csv :: pfizer_DVP_csv :: pfizer_ELIQUIS_csv ::
                    pfizer_HTN_csv :: pfizer_HTN2_csv :: pfizer_INF_csv :: pfizer_LD_csv :: pfizer_ONC_aml_csv ::
                    pfizer_ONC_other_csv :: pfizer_PAIN_C_csv :: pfizer_PAIN_lyrica_csv :: pfizer_PAIN_other_csv ::
                    pfizer_Specialty_champix_csv :: pfizer_Specialty_other_csv ::
                    pfizer_Urology_other_csv :: pfizer_Urology_viagra_csv :: pfizer_ZYVOX_csv :: Nil

        val prodMatchDF = CSV2DF(prod_match_file)
                .trim("PACK_NUMBER").trim("PACK_COUNT")
                .withColumn("PACK_NUMBER", when($"PACK_NUMBER".isNotNull, $"PACK_NUMBER").otherwise($"PACK_COUNT"))

        val productEtcDIS = prodCvs
                .toDIS(MapArgs(Map(
                    "productEtcERD" -> DFArgs(Parquet2DF(PROD_ETC_LOCATION + "/" + pfizer_company_id))
                    , "productDevERD" -> DFArgs(productDevERD)
                    , "productMatchDF" -> DFArgs(prodMatchDF)
                )))
                .getAs[DFArgs]("productEtcDIS")
                .withColumn("MIN2",
                    concat(col("DEV_PRODUCT_NAME"), col("DEV_DOSAGE_NAME"), col("DEV_PACKAGE_DES"), col("DEV_PACKAGE_NUMBER"), col("DEV_CORP_NAME"))
                )

        val marketERD = maxResultLst.map { file =>
            val maxDF = FILE2DF(file, 31.toChar.toString)
            maxDF.join(productEtcDIS, maxDF("Product") === productEtcDIS("MIN2"))
                    .select(productEtcDIS("DEV_PRODUCT_ID").as("PRODUCT_ID"), maxDF("MARKET"))
                    .distinct()
        }
                .reduce(_ unionByName _)
                .groupBy("PRODUCT_ID")
                .agg(sort_array(collect_list("MARKET")) as "MARKET")
                .withColumn("MARKET", commonUDF.mkStringByArray(col("MARKET"), lit("+")))
                .generateId
                .select("_id", "PRODUCT_ID", "MARKET")

        if(args.nonEmpty && args(0) == "TRUE"){
            marketERD.save2Mongo(PROD_MARKET_LOCATION.split("/").last)
            marketERD.save2Parquet(PROD_MARKET_LOCATION + "/" + pfizer_company_id)
        }
        marketERD.show(false)

        val marketParquet = Parquet2DF(PROD_MARKET_LOCATION + "/" + pfizer_company_id)
        phDebugLog("pfizer market:" + marketParquet.count())
        phDebugLog("pfizer market by duplicates:" + marketParquet.dropDuplicates("PRODUCT_ID").count())
    }

    phizerMarketDF()
}
