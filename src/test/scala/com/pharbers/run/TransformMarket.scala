package com.pharbers.run

import com.pharbers.util.log.phLogTrait.phDebugLog

object TransformMarket extends App {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._

    val prodDevERD = {
        Parquet2DF(PROD_DEV_LOCATION)
                .withColumn("MIN2",
                    concat(col("PRODUCT_NAME"), col("DOSAGE_NAME"), col("PACKAGE_DES"), col("PACKAGE_NUMBER"), col("CORP_NAME"))
                )
    }

    def nhwaMarketDF(): Unit = {
        val nhwa_company_id = NHWA_COMPANY_ID

        val nhwa_marketTable_csv = "/data/nhwa/pha_config_repository1809/Nhwa_MarketMatchTable_20180629.csv"

        val nhwaCpaERDDF = Parquet2DF(PROD_ETC_LOCATION + "/" + nhwa_company_id)
        val marketTableDF = CSV2DF(nhwa_marketTable_csv)

        val marketDF = nhwaCpaERDDF
                .join(
                    marketTableDF,
                    nhwaCpaERDDF("ETC_MOLE_NAME") === marketTableDF("MOLE_NAME"),
                    "left"
                )
                .withColumn("COMPANY_ID", lit(nhwa_company_id))
                .select(nhwaCpaERDDF("PRODUCT_ID"), col("COMPANY_ID"), marketTableDF("MARKET"))
                .distinct()
                .generateId

        if (args.isEmpty || args(0) == "TRUE") {
            marketDF.save2Mongo(PROD_MARKET_LOCATION.split("/").last)
            marketDF.save2Parquet(PROD_MARKET_LOCATION + "/" + nhwa_company_id)
        }

        val marketParquet = Parquet2DF(PROD_MARKET_LOCATION + "/" + nhwa_company_id)
        println("nhwa market:" + marketParquet.count())
        println("nhwa market by duplicates:" + marketParquet.dropDuplicates("PRODUCT_ID").count())
    }
    nhwaMarketDF()

    def phizerMarketDF(): Unit = {
        val pfizer_company_id = PFIZER_COMPANY_ID

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

        val pfizerMarketDF = maxResultLst.map { file =>
            val maxDF = FILE2DF(file, 31.toChar.toString)
            maxDF.join(prodDevERD, maxDF("Product") === prodDevERD("MIN2"))
                    .select(prodDevERD("_id").as("PRODUCT_ID"), maxDF("MARKET"))
                    .distinct()
        }
                .reduce(_ union _)
                .groupBy("PRODUCT_ID")
                .agg(sort_array(collect_list("MARKET")) as "MARKET")
                .withColumn("MARKET", commonUDF.mkStringByArray(col("MARKET"), lit("+")))
                .withColumn("COMPANY_ID", lit(pfizer_company_id))
                .generateId
                .select("_id", "PRODUCT_ID", "COMPANY_ID", "MARKET")

        if (args.isEmpty || args(0) == "TRUE") {
            pfizerMarketDF.save2Mongo(PROD_MARKET_LOCATION.split("/").last)
            pfizerMarketDF.save2Parquet(PROD_MARKET_LOCATION + "/" + pfizer_company_id)
        }

        val marketParquet = Parquet2DF(PROD_MARKET_LOCATION + "/" + pfizer_company_id)
        println("pfizer market:" + marketParquet.count())
        println("pfizer market by duplicates:" + marketParquet.dropDuplicates("PRODUCT_ID").count())
    }

    phizerMarketDF()
}
