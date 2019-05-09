package com.pharbers.data.run

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs, StringArgs}

object TransformProductEtc extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._

//    implicit val sparkDriver: phSparkDriver = getSparkDriver()
//    implicit val conn: spark_conn_instance = sparkDriver.conn_instance
    import com.pharbers.data.util.spark._
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

        lazy val productEtcERD = prodCvs.toERD(MapArgs(Map(
            "company_id" -> StringArgs(company_id)
            , "prodMatchDF" -> DFArgs(prodMatchDF)
            , "marketMatchDF" -> DFArgs(marketMatchDF)
            , "prodDevDF" -> DFArgs(productDevERD)
            , "matchMarketFunc" -> SingleArgFuncArgs { args: MapArgs =>
                val prodMatchDF = args.getAs[DFArgs]("prodMatchDF")
                val marketMatchDF = args.getAs[DFArgs]("marketMatchDF").select("MOLE_NAME", "MARKET")
                val resultDF = prodMatchDF.join(marketMatchDF, prodMatchDF("STANDARD_MOLE_NAME") === marketMatchDF("MOLE_NAME"), "left")
                val nullCount = resultDF.filter($"MARKET".isNull).count()
                if(nullCount != 0)
                    throw new Exception("product exist " + nullCount + " null `MARKET`")
                MapArgs(Map("result" -> DFArgs(resultDF)))
            }
            , "matchDevFunc" -> SingleArgFuncArgs(prodCvs.matchDevFunc)
        ))).getAs[DFArgs]("productEtcERD")
        lazy val productEtcERDCount = productEtcERD.count()
//        println(prodMatchDF.count(), productEtcERDCount)
//        productEtcERD.show(false)

        if(args.nonEmpty && args(0) == "TRUE")
            productEtcERD.save2Parquet(PROD_ETC_LOCATION + "/" + company_id).save2Mongo(PROD_ETC_LOCATION.split("/").last)

        lazy val productEtcDIS = prodCvs.toDIS(MapArgs(Map(
            "productEtcERD" -> DFArgs(productEtcERD) //DFArgs(Parquet2DF(PROD_ETC_LOCATION + "/" + company_id))
            , "atcERD" -> DFArgs(atcDF)
            , "productDevERD" -> DFArgs(productDevERD)
        ))).getAs[DFArgs]("productEtcDIS")
        lazy val productEtcDISCount = productEtcDIS.count()
        productEtcDIS.show(false)

        if(args.nonEmpty && args(0) == "TRUE")
            productEtcDIS.save2Parquet(PROD_ETC_DIS_LOCATION + "/" + company_id)
    }
    nhwaProdEtcDF()

    def pfizerProdEtcDF(): Unit = {
        lazy val company_id = PFIZER_COMPANY_ID

        lazy val prod_match_file = "/data/pfizer/pha_config_repository1901/Pfizer_ProductMatchTable_20190403.csv"
        lazy val market_match_file = "/data/pfizer/pha_config_repository1901/Pfizer_MarketMatchTable_20190422.csv"

        lazy val prodMatchDF = CSV2DF(prod_match_file)
                .addColumn("PACK_NUMBER").addColumn("PACK_COUNT")
                .withColumn("PACK_COUNT", when($"PACK_NUMBER".isNotNull, $"PACK_NUMBER").otherwise($"PACK_COUNT"))
        lazy val marketMatchDF = CSV2DF(market_match_file)
        marketMatchDF.groupBy("MOLE_NAME")
                .agg(sort_array(collect_list("MARKET")) as "MARKET", countDistinct($"MARKET") as "count")
                .sort(col("count").desc)
                .show(false)

        lazy val productEtcERD = prodCvs.toERD(MapArgs(Map(
            "company_id" -> StringArgs(company_id)
            , "prodMatchDF" -> DFArgs(prodMatchDF)
            , "marketMatchDF" -> DFArgs(marketMatchDF)
            , "prodDevDF" -> DFArgs(productDevERD)
            , "matchMarketFunc" -> SingleArgFuncArgs { args: MapArgs =>
                val prodMatchDF = args.getAs[DFArgs]("prodMatchDF")
                val marketMatchDF = args.getAs[DFArgs]("marketMatchDF").select("MOLE_NAME", "MARKET")
                val resultDF = prodMatchDF.join(marketMatchDF, prodMatchDF("STANDARD_MOLE_NAME") === marketMatchDF("MOLE_NAME"), "left")
                val nullCount = resultDF.filter($"MARKET".isNull).count()
                if(nullCount != 0)
                    throw new Exception("product exist " + nullCount + " null `MARKET`")
                MapArgs(Map("result" -> DFArgs(resultDF)))
            }
            , "matchDevFunc" -> SingleArgFuncArgs(prodCvs.matchDevFunc)
        ))).getAs[DFArgs]("productEtcERD")
        lazy val productEtcERDCount = productEtcERD.count()
        productEtcERD.show(false)
        println(prodMatchDF.count(), productEtcERDCount)

        if(args.nonEmpty && args(0) == "TRUE")
            productEtcERD.save2Parquet(PROD_ETC_LOCATION + "/" + company_id).save2Mongo(PROD_ETC_LOCATION.split("/").last)

        lazy val productEtcDIS = prodCvs.toDIS(MapArgs(Map(
            "productEtcERD" -> DFArgs(productEtcERD) //DFArgs(Parquet2DF(PROD_ETC_LOCATION + "/" + company_id))
            , "productDevERD" -> DFArgs(productDevERD)
        ))).getAs[DFArgs]("productEtcDIS")
        lazy val productEtcDISCount = productEtcDIS.count()
        productEtcDIS.show(false)

        productEtcDIS.groupBy("DEV_MOLE_NAME")
                .agg(sort_array(collect_list("MARKET")) as "MARKET", countDistinct($"MARKET") as "count")
                .sort(col("count").desc)
                .show(false)

        if(args.nonEmpty && args(0) == "TRUE")
            productEtcDIS.save2Parquet(PROD_ETC_DIS_LOCATION + "/" + company_id)
    }
//    pfizerProdEtcDF()

}