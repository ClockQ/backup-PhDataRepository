package com.pharbers.data.conversion

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

/**
  * @description: product of pharbers
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class ProductEtcConversion() extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    override def toERD(args: MapArgs): MapArgs = {

        val sourceDataDF = args.get.getOrElse("sourceDataDF", throw new Exception("not found sourceDataDF")).getBy[DFArgs]

        val prodERD = sourceDataDF
                .select("COMPANY_ID", "SOURCE", "PRODUCT_NAME", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME")
                // 1. SOURCE
                .groupBy("COMPANY_ID", "PRODUCT_NAME", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME")
                .agg(sort_array(collect_list("SOURCE")) as "SOURCE")
                .withColumn("SOURCE", commonUDF.mkStringByArray($"SOURCE", lit("+")))
                // 2. MIN1
                .withColumn("MIN1", concat(col("PRODUCT_NAME"), col("DOSAGE"), col("PACK_DES"), col("PACK_NUMBER"), col("CORP_NAME")))
                .select(
                    $"COMPANY_ID" as "ETC_COMPANY_ID",
                    $"SOURCE" as "ETC_SOURCE",
                    $"PRODUCT_NAME" as "ETC_PRODUCT_NAME",
                    $"CORP_NAME" as "ETC_CORP_NAME",
                    $"MOLE_NAME" as "ETC_MOLE_NAME",
                    $"PACK_DES" as "ETC_PACKAGE_DES",
                    $"PACK_NUMBER" as "ETC_PACKAGE_NUMBER",
                    $"DOSAGE" as "ETC_DOSAGE_NAME",
                    $"DELIVERY_WAY" as "ETC_DELIVERY_WAY"
                )
                .generateId

        MapArgs(Map(
            "productEtcERD" -> DFArgs(prodERD)
        ))
    }

    override def toDIS(args: MapArgs): MapArgs = {
        val productEtcERD = args.get.getOrElse("productEtcERD", throw new Exception("not found productEtcERD")).getBy[DFArgs]
        val atcERD = args.get.get("atcERD")
        val marketERD = args.get.get("marketERD")
        val productDevERD = args.get.get("productDevERD")

        val etcConnDevDF = productDevERD match {
            case Some(dev) =>
                val productMatchDF = args.get("productMatchDF").getBy[DFArgs]
                val productDevDF = {
                    dev.getBy[DFArgs].withColumnRenamed("_id", "DEV_PRODUCT_ID")
                            .select(col("DEV_PRODUCT_ID"), col("DEV_PRODUCT_NAME"), col("DEV_DOSAGE_NAME"),
                                col("DEV_PACKAGE_DES"), col("DEV_PACKAGE_NUMBER"), col("DEV_CORP_NAME"))
                            .withColumn("MIN2", concat(
                                col("DEV_PRODUCT_NAME"),
                                col("DEV_DOSAGE_NAME"),
                                col("DEV_PACKAGE_DES"),
                                col("DEV_PACKAGE_NUMBER"),
                                col("DEV_CORP_NAME"))
                            )
                            .dropDuplicates("MIN2")
                }

                productEtcERD
                        .withColumn("ETC_PRODUCT_ID", $"_id")
                        .withColumn("MIN1", concat(
                            col("ETC_PRODUCT_NAME"),
                            col("ETC_DOSAGE_NAME"),
                            col("ETC_PACKAGE_DES"),
                            col("ETC_PACKAGE_NUMBER"),
                            col("ETC_CORP_NAME"))
                        )
                        .join(productMatchDF
                                .select("MIN_PRODUCT_UNIT", "MIN_PRODUCT_UNIT_STANDARD")
                                .dropDuplicates("MIN_PRODUCT_UNIT"),
                            col("MIN1") === col("MIN_PRODUCT_UNIT"), "left"
                        )
                        .join(productDevDF
                            , productMatchDF("MIN_PRODUCT_UNIT_STANDARD") === productDevDF("MIN2")
                            , "left"
                        )
                        .drop("MIN1")
                        .drop("MIN_PRODUCT_UNIT")
                        .drop("MIN_PRODUCT_UNIT_STANDARD")
                        .drop("MIN2")

            case None => productEtcERD
        }

        val etcConnAtcDF = atcERD match {
            case Some(atc) =>
                val atcDF = atc.getBy[DFArgs].dropDuplicates("MOLE_NAME")
                etcConnDevDF
                        .join(
                            atcDF
                            , col("ETC_MOLE_NAME") === atcDF("MOLE_NAME")
                            , "left"
                        )
                        .drop(atcDF("_id"))
                        .drop(atcDF("MOLE_NAME"))
            case None => etcConnDevDF
        }

        val etcConnMarketERD = marketERD match {
            case Some(market) =>
                val marketDF = market.getBy[DFArgs]
                etcConnAtcDF
                        .join(
                            marketDF
                            , etcConnAtcDF("DEV_PRODUCT_ID") === marketDF("PRODUCT_ID")
                            , "left"
                        )
                        .drop(marketDF("_id"))
                        .drop(marketDF("PRODUCT_ID"))
            case None => etcConnAtcDF
        }

        MapArgs(Map("productEtcDIS" -> DFArgs(etcConnMarketERD)))
    }
}
