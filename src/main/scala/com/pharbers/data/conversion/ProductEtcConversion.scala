package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame
import com.pharbers.data.util.commonUDF

/**
  * @description: product of pharbers
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class ProductEtcConversion(old: String = "") extends PhDataConversion {

    import com.pharbers.data.util.DFUtil
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {

        val sourceDataDF = args.getOrElse("sourceDataDF", throw new Exception("not found sourceDataDF"))
        val productDevERD = args.getOrElse("productDevERD", throw new Exception("not found productDevERD"))
        val productMatchDF = args.getOrElse("productMatchDF", throw new Exception("not found productMatchDF"))

        val prodERD = sourceDataDF
                .select("COMPANY_ID", "SOURCE", "PRODUCT_NAME", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME")
                // 1. SOURCE
                .groupBy("COMPANY_ID", "PRODUCT_NAME", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME")
                .agg(sort_array(collect_list("SOURCE")) as "SOURCE")
                .withColumn("SOURCE", commonUDF.mkStringByArray($"SOURCE", lit("+")))
                // 2. MIN1
                .withColumn("MIN1", concat(col("PRODUCT_NAME"), col("DOSAGE"), col("PACK_DES"), col("PACK_NUMBER"), col("CORP_NAME")))
                .join(productMatchDF
                        .select("MIN_PRODUCT_UNIT", "MIN_PRODUCT_UNIT_STANDARD")
                        .dropDuplicates("MIN_PRODUCT_UNIT")
                        .distinct()
                    , col("MIN1") === col("MIN_PRODUCT_UNIT"), "left")
                .join(productDevERD
                        .withColumnRenamed("_id", "PRODUCT_ID")
                        .select(col("PRODUCT_ID"), col("PRODUCT_NAME"), col("DOSAGE_NAME"), col("PACKAGE_DES"), col("PACKAGE_NUMBER"), col("CORP_NAME"))
                        .withColumn("MIN2", concat(col("PRODUCT_NAME"), col("DOSAGE_NAME"), col("PACKAGE_DES"), col("PACKAGE_NUMBER"), col("CORP_NAME")))
                        .drop("PRODUCT_NAME")
                        .drop("DOSAGE_NAME")
                        .drop("PACKAGE_DES")
                        .drop("PACKAGE_NUMBER")
                        .drop("CORP_NAME")
                        .dropDuplicates("MIN2")
                    , col("MIN_PRODUCT_UNIT_STANDARD") === col("MIN2"), "left")
                .na.fill("")
                .select(
                    $"PRODUCT_ID",
                    $"COMPANY_ID",
                    $"SOURCE" as "ETC_SOURCE",
                    $"PRODUCT_NAME" as "ETC_PRODUCT_NAME",
                    $"MOLE_NAME" as "ETC_MOLE_NAME",
                    $"PACK_DES" as "ETC_PACKAGE_DES",
                    $"PACK_NUMBER" as "ETC_PACKAGE_NUMBER",
                    $"DOSAGE" as "ETC_DOSAGE_NAME",
                    $"DELIVERY_WAY" as "ETC_DELIVERY_WAY",
                    $"CORP_NAME" as "ETC_CORP_NAME"
                )
                .generateId

        Map(
            "productEtcERD" -> prodERD
        )
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val productEtcERD = args.getOrElse("productEtcERD", throw new Exception("not found productEtcERD"))
        val marketERD = args.getOrElse("marketERD", Seq.empty[(String, String, String, String)].toDF("_id", "PRODUCT_ID", "COMPANY_ID", "MARKET"))
        val atcERD = args.getOrElse("atcERD", Seq.empty[(String, String, String)].toDF("_id", "MOLE_NAME", "ATC_CODE"))

        val productEtcDIS = productEtcERD
                .join(
                    marketERD,
                    productEtcERD("PRODUCT_ID") === marketERD("PRODUCT_ID") &&
                    productEtcERD("COMPANY_ID") === marketERD("COMPANY_ID"),
                    "left"
                )
                .drop(marketERD("_id"))
                .drop(marketERD("PRODUCT_ID"))
                .drop(marketERD("COMPANY_ID"))
                .join(
                    atcERD.dropDuplicates("MOLE_NAME"),
                    col("ETC_MOLE_NAME") === atcERD("MOLE_NAME"),
                    "left"
                )
                .drop(atcERD("_id"))
                .drop(atcERD("MOLE_NAME"))

        Map(
            "productEtcDIS" -> productEtcDIS
        )
    }
}
