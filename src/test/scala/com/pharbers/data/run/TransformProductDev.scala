package com.pharbers.data.run

import org.apache.spark.sql.DataFrame
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

/**
  * @description:
  * @author: clock
  * @date: 2019-04-16 17:50
  */
object TransformProductDev extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
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
                , $"PACK_ID" as "DEV_PACK_ID"
            )

    def chc2Product(df: DataFrame): DataFrame = df
            .trim("DEV_PACKAGE_DES")
            .trim("DEV_PACKAGE_NUMBER")
            .trim("DEV_DOSAGE_NAME")
            .select(
                $"Prod_Desc" as "DEV_PRODUCT_NAME"
                , $"MNF_Desc" as "DEV_CORP_NAME"
                , $"Molecule_Desc" as "DEV_MOLE_NAME"
                , $"DEV_PACKAGE_DES"
                , $"DEV_PACKAGE_NUMBER"
                , $"DEV_DOSAGE_NAME"
                , $"Pack_ID" as "DEV_PACK_ID"
            )

    val nhwaProductMatchFile = "/data/nhwa/pha_config_repository1809/Nhwa_ProductMatchTable_20181126.csv"
    val pfizerProductMatchFile = "/data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv"

    val chcFile = "/test/OAD CHC data for 5 cities to 2018Q3 v3.csv"
    val packIdFile1 = "/test/chc/CHC_packid匹配表1.csv"
    val packIdFile2 = "/test/chc/CHC_packid匹配表2.csv"
    val packIdFile3 = "/test/chc/CHC_packid匹配表3.csv"

    val chcDF = chc2Product(CSV2DF(chcFile))

    val packIdDF = CSV2DF(packIdFile1).select(
        $"商品名" as "DEV_PRODUCT_NAME"
        , $"生产企业名称" as "DEV_CORP_NAME"
        , $"化学名" as "DEV_MOLE_NAME"
        , $"标准规格" as "DEV_PACKAGE_DES"
        , $"转换比" as "DEV_PACKAGE_NUMBER"
        , $"剂型" as "DEV_DOSAGE_NAME"
        , $"PFC" as "DEV_PACK_ID"
    ) unionByName CSV2DF(packIdFile2).select(
        $"Brand" as "DEV_PRODUCT_NAME"
        , $"Manufacturer" as "DEV_CORP_NAME"
        , $"Molecule" as "DEV_MOLE_NAME"
        , $"Specification" as "DEV_PACKAGE_DES"
        , $"Pack_number" as "DEV_PACKAGE_NUMBER"
        , $"Form" as "DEV_DOSAGE_NAME"
        , $"pack ID" as "DEV_PACK_ID"
    ) unionByName CSV2DF(packIdFile3).select(
        $"Brand" as "DEV_PRODUCT_NAME"
        , $"Manufacturer" as "DEV_CORP_NAME"
        , $"Molecule" as "DEV_MOLE_NAME"
        , $"Specification" as "DEV_PACKAGE_DES"
        , $"Pack_number" as "DEV_PACKAGE_NUMBER"
        , $"Form" as "DEV_DOSAGE_NAME"
        , $"packcode" as "DEV_PACK_ID"
    )

    val nhwaMatchDF = matchTable2Product(CSV2DF(nhwaProductMatchFile).withColumnRenamed("PACK_COUNT", "PACK_NUMBER"))

    val pfizerMatchDF = matchTable2Product(CSV2DF(pfizerProductMatchFile))

    val pdc = ProductDevConversion()

    val productDevERD: DataFrame = pdc.toERD(MapArgs(Map(
        "nhwaMatchDF" -> DFArgs(nhwaMatchDF)
        , "pfizerMatchDF" -> DFArgs(pfizerMatchDF)
        , "chcDF" -> DFArgs(chcDF)
        , "packIdDF" -> DFArgs(packIdDF)
    ))).getAs[DFArgs]("productDevERD")

//    productDevERD.show(false)
//    productDevERD.groupBy($"DEV_PACK_ID").count().sort($"count".desc).show(false)
//    println(productDevERD.count())
//    productDevERD.filter($"DEV_PACK_ID" === "3801002").show(false)

    if (args.isEmpty || args(0) == "TRUE") {
        productDevERD.save2Mongo(PROD_DEV_LOCATION.split("/").last)
        productDevERD.save2Parquet(PROD_DEV_LOCATION)
    }
}
