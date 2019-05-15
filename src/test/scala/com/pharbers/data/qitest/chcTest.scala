package com.pharbers.data.qitest

import com.pharbers.data.util.ParquetLocation.HOSP_PHA_LOCATION
import org.apache.spark.sql.functions._

/**
  * @description:
  * @author: clock
  * @date: 2019-04-29 13:50
  */
object chcTest extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.util.spark._
    import com.pharbers.data.util.spark.sparkDriver.ss.implicits._

    lazy val imsPackFile = "/test/IMS_PACK_ID.csv"
    lazy val chc_oad_q4 = "/test/chc/OAD CHC data for 5 cities to 2018Q4.csv"
    lazy val chc_ca_file = "/test/CHC_CA_5cities_Deliverable.csv"

    lazy val imsDF = CSV2DF(imsPackFile)
    lazy val chcOadDF = CSV2DF(chc_oad_q4)
    lazy val chcCaDF = CSV2DF(chc_ca_file)

    lazy val a = imsDF.na.fill("")
            .withColumn("PCK_DESC", $"Pck_Desc")
            .withColumn("DEV_PRODUCT_NAME", when($"product_cn" === "", lit(null)).otherwise($"product_cn"))
            .withColumn("DEV_CORP_NAME", when($"ims_corp" === "", lit(null)).otherwise($"ims_corp"))
            .withColumn("DEV_MOLE_NAME", when($"Mole_name" === "", lit(null)).otherwise($"Mole_name"))
            .withColumn("DEV_PACKAGE_DES", commonUDF.trimUdf(
                commonUDF.mkStringUdf(array("Str_Desc", "PckVol_Desc"), lit(" "))
            ))
            .withColumn("DEV_PACKAGE_NUMBER", when($"PckSize_Desc" === 0, lit(null)).otherwise($"PckSize_Desc"))
            .withColumn("DEV_DELIVERY_WAY", when($"NFC123_Code" === "", lit(null)).otherwise($"NFC123_Code"))
            .withColumn("DEV_PACK_ID", when($"Pack_Id0" === "", lit(null)).otherwise($"Pack_Id0"))
            .withColumn("DEV_DOSAGE_NAME",
                when(
                    col("DEV_PACKAGE_DES").isNotNull && col("DEV_PACKAGE_DES") =!= "",
                    commonUDF.headUdf(commonUDF.splitUdf(col("PCK_DESC"), col("DEV_PACKAGE_DES")))
                ).otherwise(
                    when(
                        col("DEV_PACKAGE_NUMBER").isNotNull,
                        commonUDF.headUdf(commonUDF.splitUdf(col("PCK_DESC"), col("DEV_PACKAGE_NUMBER")))
                    ).otherwise(col("PCK_DESC"))
                )
            )
            .select(
                "DEV_PRODUCT_NAME", "DEV_CORP_NAME", "DEV_MOLE_NAME", "DEV_PACKAGE_DES",
                "DEV_PACKAGE_NUMBER", "DEV_DOSAGE_NAME", "DEV_DELIVERY_WAY", "DEV_PACK_ID"
            )

    lazy val b = a.groupBy("DEV_PRODUCT_NAME", "DEV_CORP_NAME", "DEV_MOLE_NAME",
                "DEV_PACKAGE_DES", "DEV_PACKAGE_NUMBER", "DEV_DELIVERY_WAY", "DEV_DOSAGE_NAME")
            .agg(sort_array(collect_list("DEV_PACK_ID")) as "DEV_PACK_ID", countDistinct($"DEV_PACK_ID") as "count")
            .sort(col("count").desc)
            .filter($"count" > 1)

    val c = chcOadDF.join(imsDF, chcOadDF("Pack_ID") === imsDF("Pack_Id0"), "left")
            .withColumn("arr", array(col("分子"), col("Mole_name")))
                    .groupBy("Pack_ID")
                    .agg(countDistinct("arr") as "count")
                    .sort(col("count").desc)
//                    .filter(imsDF("product_cn").isNull)
//            .filter(imsDF("Pack_Id0").isNull)
//                    .filter(imsDF("product_cn") === "#N/A")
    c.show(false)

}
