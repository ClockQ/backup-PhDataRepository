package com.pharbers.data.run

import org.apache.spark.sql.DataFrame
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs}

/**
  * @description:
  * @author: clock
  * @date: 2019-04-16 19:24
  */
object TransformCHC extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    val chcFile1 = "/test/OAD CHC data for 5 cities to 2018Q3 v3.csv"
    val chcFile2 = "/test/chc/OAD CHC data for 5 cities to 2018Q4.csv"
    val chc_ca_file = "/test/CHC_CA_5cities_Deliverable.csv"

    val piCvs = ProductImsConversion()
    val chcCvs = CHCConversion()

    val chcDF = CSV2DF(chcFile2) // 8728 CSV2DF(chcFile1) unionByName
    val chcDFCount = chcDF.count()
    val cityDF = Parquet2DF(HOSP_ADDRESS_CITY_LOCATION)

    val productImsDIS = piCvs.toDIS(MapArgs(Map(
        "productImsERD" -> DFArgs(Parquet2DF(PROD_IMS_LOCATION))
        , "atc3ERD" -> DFArgs(Parquet2DF(PROD_ATC3TABLE_LOCATION))
        , "oadERD" -> DFArgs(Parquet2DF(PROD_OADTABLE_LOCATION))
        , "productDevERD" -> DFArgs(Parquet2DF(PROD_DEV_LOCATION))
    ))).getAs[DFArgs]("productImsDIS")

    val chcERD = chcCvs.toERD(MapArgs(Map(
        "chcDF" -> DFArgs(chcDF)
        , "dateDF" -> DFArgs(Parquet2DF(CHC_DATE_LOCATION))
        , "cityDF" -> DFArgs(cityDF)
        , "productDIS" -> DFArgs(productImsDIS)
        , "addCHCProdFunc" -> SingleArgFuncArgs { df: DataFrame =>
            ProductDevConversion2().toERD(MapArgs(Map(
                "chcDF" -> DFArgs(df)
            ))).getAs[DFArgs]("productDevERD")
        }
    ))).getAs[DFArgs]("chcERD")
    val chcERDCount = chcERD.count()
    chcERD.show(false)

    val chcProdIsNullCount = chcERD.filter($"PRODUCT_ID".isNull).count()
    assert(chcProdIsNullCount == 0, "chc: 转换后的ERD有`" + chcProdIsNullCount + "`条产品未匹配")

    val chcErdMinus = chcDFCount - chcERDCount
    assert(chcErdMinus == 0, "chc: 转换后的ERD比源数据减少`" + chcErdMinus + "`条记录")

    if(args.nonEmpty && args(0) == "TRUE")
        chcERD.save2Parquet(CHC_LOCATION).save2Mongo(CHC_LOCATION.split("/").last)

    val chcDIS = chcCvs.toDIS(MapArgs(Map(
        "chcERD" -> DFArgs(Parquet2DF(CHC_LOCATION))
        , "dateERD" -> DFArgs(Parquet2DF(CHC_DATE_LOCATION))
        , "cityERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_CITY_LOCATION))
        , "productDIS" -> DFArgs(productImsDIS)
    ))).getAs[DFArgs]("chcDIS")
    val chcDISCount = chcDIS.count() // 8728
    chcDIS.show(false)
    chcCvs.toCHCStruct(chcDIS).show(false)

    val chcDisMinus = chcDFCount - chcDISCount
    assert(chcDisMinus == 0, "chc: 转换后的DIS比源数据减少`" + chcDisMinus + "`条记录")

    def appendChcProduct(chcERD: DataFrame): Unit = {

        val prodIdIsNull = chcERD.filter($"DEV_PRODUCT_NAME".isNull)
        println(prodIdIsNull.count())
        prodIdIsNull.show(false)
        def chc2Product(df: DataFrame): DataFrame = df
                .addColumn("DEV_PACKAGE_DES")
                .addColumn("DEV_PACKAGE_NUMBER")
                .addColumn("DEV_DOSAGE_NAME")
                .select(
                    $"IMS_PRODUCT_NAME" as "DEV_PRODUCT_NAME"
                    , $"IMS_CORP_NAME" as "DEV_CORP_NAME"
                    , $"IMS_MOLE_NAME" as "DEV_MOLE_NAME"
                    , $"DEV_PACKAGE_DES"
                    , $"DEV_PACKAGE_NUMBER"
                    , $"DEV_DOSAGE_NAME"
                    , $"IMS_PACK_ID" as "DEV_PACK_ID"
                )

        val pdc = ProductDevConversion2()

        val productDevERD: DataFrame = pdc.toERD(MapArgs(Map(
            "chcDF" -> DFArgs(chc2Product(prodIdIsNull))
        ))).getAs[DFArgs]("productDevERD")
        println(productDevERD.count())
        if(args.nonEmpty && args(0) == "TRUE"){
            productDevERD.save2Mongo(PROD_DEV_LOCATION.split("/").last)
            productDevERD.save2Parquet(PROD_DEV_LOCATION)
        }
    }
}
