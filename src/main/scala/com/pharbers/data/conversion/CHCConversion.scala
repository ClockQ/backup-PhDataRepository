package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

case class CHCConversion() extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    override def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val chcDF = args.getOrElse("chcDF", throw new Exception("not found chcDF"))
        val prodDF = args.getOrElse("prodDF", throw new Exception("not found prodDF"))
                .dropDuplicates("PACK_ID")
        val cityDF = args.getOrElse("cityDF", throw new Exception("not found cityDF"))
                .select($"_id".as("CITY_ID"), regexp_replace($"name", "市", "").as("NAME"))
                .dropDuplicates("NAME")

        val dateDF = chcDF.select($"Date".as("TIME"))
                .distinct()
                .withColumn("PERIOD", lit("quarter"))
                .generateId
                .cache()

        val chcERD = {
            chcDF
                    // DATE_ID
                    .join(
                        dateDF.withColumnRenamed("_id", "DATE_ID"),
                        chcDF("Date") === dateDF("TIME"), "left")
                    // PRODUCT_ID
                    .join(
                        prodDF.select($"_id".as("PRODUCT_ID"), $"PACK_ID"),
                        chcDF("Pack_ID") === prodDF("PACK_ID"), "left")
                    // CITY_ID
                    .join(
                        cityDF,
                        chcDF("city") === cityDF("NAME"), "left")
                    // Adjust the order
                    .select($"PRODUCT_ID", $"CITY_ID", $"DATE_ID", $"Sales".as("SALES"), $"Units".as("UNITS"))
                    .generateId
        }

        Map(
            "chcERD" -> chcERD,
            "dateERD" -> dateDF
        )
    }

    override def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val chcERD = args.getOrElse("chcERD", throw new Exception("not found chcERD"))
        val dateERD = args.getOrElse("dateERD", throw new Exception("not found dateERD"))
        val cityERD = args.getOrElse("cityERD", throw new Exception("not found cityERD"))
        val oadERD = args.getOrElse("oadERD", throw new Exception("not found oadERD"))
        val atc3ERD = args.getOrElse("atc3ERD", throw new Exception("not found atc3ERD"))
        val productDIS = args.getOrElse("productDIS", throw new Exception("not found productDIS"))

        val chcDIS = {
            chcERD
                    .join(
                        productDIS,
                        chcERD("PRODUCT_ID") === productDIS("_id"),
                        "left"
                    ).drop(productDIS("_id"))
                    .join(
                        dateERD,
                        chcERD("DATE_ID") === dateERD("_id"),
                        "left"
                    ).drop(dateERD("_id"))
                    .join(
                        cityERD,
                        chcERD("CITY_ID") === cityERD("_id"),
                        "left"
                    ).drop(cityERD("_id"))
                    .join(
                        atc3ERD,
                        productDIS("PACK_ID") === atc3ERD("PACK_ID"),
                        "left"
                    ).drop(atc3ERD("PACK_ID")).drop(atc3ERD("_id"))
                    .join(
                        oadERD,
                        atc3ERD("ATC3") === oadERD("ATC3"),
                        "left"
                    ).drop(oadERD("ATC3")).drop(oadERD("_id"))
        }

        Map("chcDIS" -> chcDIS)
    }

    def toCHCStruct(dis: DataFrame): DataFrame =
        dis.select(
            $"PACK_ID", $"TIME", $"name"
            , $"PRODUCT_NAME", $"MOLE_NAME", $"CORP_NAME"
            , $"PACKAGE_NUMBER", $"DOSAGE_NAME", $"PACKAGE_DES"
            , $"ATC3", $"OAD_TYPE"
            , $"SALES", $"UNITS"
        )
}
