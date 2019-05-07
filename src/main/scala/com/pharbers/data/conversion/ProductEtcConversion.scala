package com.pharbers.data.conversion

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs, StringArgs}

/**
  * @description: product of pharbers
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class ProductEtcConversion() extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    override def file2ERD(args: MapArgs): MapArgs = {
        val company_id = args.getAs[StringArgs]("company_id")
        val matchMarketFunc = args.getAs[SingleArgFuncArgs[MapArgs, MapArgs]]("matchMarketFunc")
        val matchDevFunc = args.getAs[SingleArgFuncArgs[MapArgs, MapArgs]]("matchDevFunc")

        val matchMarketProdDF = matchMarketFunc(args).get.head._2.getBy[DFArgs]

        val matchDevProdDF = matchDevFunc(
            MapArgs(args.get + ("prodMatchDF" -> DFArgs(matchMarketProdDF)))
        ).get.head._2.getBy[DFArgs]

        val productEtcERD = matchDevProdDF
                .select(
                    lit(company_id) as "ETC_COMPANY_ID"
                    , $"STANDARD_PRODUCT_NAME" as "ETC_PRODUCT_NAME"
                    , $"STANDARD_CORP_NAME" as "ETC_CORP_NAME"
                    , $"STANDARD_MOLE_NAME" as "ETC_MOLE_NAME"
                    , $"STANDARD_PACK_DES" as "ETC_PACKAGE_DES"
                    , $"PACK_COUNT" as "ETC_PACKAGE_NUMBER"
                    , $"STANDARD_DOSAGE" as "ETC_DOSAGE_NAME"
                    , lit("") as "ETC_DELIVERY_WAY"
                    , $"MARKET"
                    , $"DEV_PRODUCT_ID"
                )
                .dropDuplicates("DEV_PRODUCT_ID")
                .generateId

        MapArgs(Map(
            "productEtcERD" -> DFArgs(productEtcERD)
        ))
    }

    override def extractByDIS(args: MapArgs): MapArgs = ???

    override def mergeERD(args: MapArgs): MapArgs = {
        val productEtcERD = args.get.getOrElse("productEtcERD", throw new Exception("not found productEtcERD"))
                .getBy[DFArgs].withColumn("ETC_PRODUCT_ID", $"_id")
        val atcERD = args.get.get("atcERD")
        val productDevERD = args.get.get("productDevERD")

        val etcConnAtcDF = atcERD match {
            case Some(atc) =>
                val atcDF = atc.getBy[DFArgs].dropDuplicates("MOLE_NAME")
                productEtcERD
                        .join(
                            atcDF
                            , col("ETC_MOLE_NAME") === atcDF("MOLE_NAME")
                            , "left"
                        )
                        .drop(atcDF("_id"))
                        .drop(atcDF("MOLE_NAME"))
            case None => productEtcERD
        }

        val etcConnDevDF = productDevERD match {
            case Some(dev) =>
                val devDF = dev.getBy[DFArgs]
                etcConnAtcDF
                        .join(
                            devDF
                            , col("DEV_PRODUCT_ID") === devDF("_id")
                            , "left"
                        )
                        .drop(devDF("_id"))
            case None => etcConnAtcDF
        }

        MapArgs(Map("productEtcDIS" -> DFArgs(etcConnDevDF)))
    }
}
