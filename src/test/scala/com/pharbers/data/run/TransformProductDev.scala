package com.pharbers.data.run

import org.apache.spark.sql.DataFrame
import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

/**
  * @description:
  * @author: clock
  * @date: 2019-04-16 17:50
  */
object TransformProductDev extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    def matchTable2Product(df: DataFrame): DataFrame = df
            .select(
                $"STANDARD_PRODUCT_NAME" as "DEV_PRODUCT_NAME"
                , $"STANDARD_CORP_NAME" as "DEV_CORP_NAME"
                , $"STANDARD_MOLE_NAME" as "DEV_MOLE_NAME"
                , $"STANDARD_PACK_DES" as "DEV_PACKAGE_DES"
                , $"PACK_NUMBER" as "DEV_PACKAGE_NUMBER"
                , $"STANDARD_DOSAGE" as "DEV_DOSAGE_NAME"
                , lit(null) as "DEV_DELIVERY_WAY"
                , $"PACK_ID" as "DEV_PACK_ID"
            )

    lazy val nhwaProductMatchFile = "/data/nhwa/pha_config_repository1809/Nhwa_ProductMatchTable_20181126.csv"
    lazy val pfizerProductMatchFile = "/data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv"

    lazy val packIdFile1 = "/test/chc/CHC_packid匹配表1.csv"
    lazy val packIdFile2 = "/test/chc/CHC_packid匹配表2.csv"
    lazy val packIdFile3 = "/test/chc/CHC_packid匹配表3.csv"

    lazy val imsPackFile = "/test/IMS_PACK_ID.csv"

    lazy val packIdDF = CSV2DF(packIdFile1).select(
        $"商品名" as "DEV_PRODUCT_NAME"
        , $"生产企业名称" as "DEV_CORP_NAME"
        , $"化学名" as "DEV_MOLE_NAME"
        , $"标准规格" as "DEV_PACKAGE_DES"
        , $"转换比" as "DEV_PACKAGE_NUMBER"
        , $"剂型" as "DEV_DOSAGE_NAME"
        , lit(null) as "DEV_DELIVERY_WAY"
        , $"PFC" as "DEV_PACK_ID"
    ) unionByName CSV2DF(packIdFile2).select(
        $"Brand" as "DEV_PRODUCT_NAME"
        , $"Manufacturer" as "DEV_CORP_NAME"
        , $"Molecule" as "DEV_MOLE_NAME"
        , $"Specification" as "DEV_PACKAGE_DES"
        , $"Pack_number" as "DEV_PACKAGE_NUMBER"
        , $"Form" as "DEV_DOSAGE_NAME"
        , lit(null) as "DEV_DELIVERY_WAY"
        , $"pack ID" as "DEV_PACK_ID"
    ) unionByName CSV2DF(packIdFile3).select(
        $"Brand" as "DEV_PRODUCT_NAME"
        , $"Manufacturer" as "DEV_CORP_NAME"
        , $"Molecule" as "DEV_MOLE_NAME"
        , $"Specification" as "DEV_PACKAGE_DES"
        , $"Pack_number" as "DEV_PACKAGE_NUMBER"
        , $"Form" as "DEV_DOSAGE_NAME"
        , lit(null) as "DEV_DELIVERY_WAY"
        , $"packcode" as "DEV_PACK_ID"
    )

    lazy val nhwaMatchDF = matchTable2Product(CSV2DF(nhwaProductMatchFile).withColumnRenamed("PACK_COUNT", "PACK_NUMBER"))

    lazy val pfizerMatchDF = matchTable2Product(CSV2DF(pfizerProductMatchFile))

    lazy val pdc = ProductDevConversion()

    lazy val imsPackIdDF = {
        CSV2DF(imsPackFile)
                .na.fill("")
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
                        ).otherwise(col("Pck_Desc"))
                    )
                )
                .select(
                    "DEV_PRODUCT_NAME", "DEV_CORP_NAME", "DEV_MOLE_NAME", "DEV_PACKAGE_DES",
                    "DEV_PACKAGE_NUMBER", "DEV_DOSAGE_NAME", "DEV_DELIVERY_WAY", "DEV_PACK_ID"
                )
    }

    val productDevERD: DataFrame = pdc.toERD(MapArgs(Map(
//        "nhwaMatchDF" -> DFArgs(nhwaMatchDF)
//        , "pfizerMatchDF" -> DFArgs(pfizerMatchDF)
//        , "packIdDF" -> DFArgs(packIdDF)
        "imsPackIdDF" -> DFArgs(imsPackIdDF)
    ))).getAs[DFArgs]("productDevERD")
    productDevERD.show(false)
    val productDevERDCount = productDevERD.count()

    phDebugLog(s"imsPackIdDF count = ${imsPackIdDF.count()}, productDevERD count = $productDevERDCount")

//    if (args.nonEmpty && args(0) == "TRUE")
        productDevERD.save2Mongo(PROD_DEV_LOCATION.split("/").last).save2Parquet(PROD_DEV_LOCATION)

}
