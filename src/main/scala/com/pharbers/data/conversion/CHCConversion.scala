package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

case class CHCConversion() extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    override def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val chcDF = args.getOrElse("chcDF", throw new Exception("not found chcDF"))
                .select(
                    $"Pack_ID".as("PACK_ID"), $"city".as("CITY")
                    , $"Date", $"ATC3", $"OAD类别", $"Sales", $"Units"
                )
        val prodDF = args.getOrElse("prodDF", throw new Exception("not found prodDF"))
        val cityDF = args.getOrElse("cityDF", throw new Exception("not found cityDF"))
                .select($"_id".as("CITY_ID"), regexp_replace($"name", "市", "").as("NAME"))

        val dateDF = chcDF.select($"Date".as("TIME"))
                .distinct()
                .withColumn("PERIOD", lit("quarter"))
                .generateId
                .withColumnRenamed("_id", "DATE_ID")

        val chcERD = {
            chcDF
                    // DATE_ID
                    .join(dateDF, chcDF("Date") === dateDF("TIME"))
                    .drop(chcDF("Date"))
                    .drop(dateDF("TIME"))
                    .drop(dateDF("PERIOD"))
                    // PRODUCT_ID
                    .join(
                        prodDF.select($"_id".as("PRODUCT_ID"), $"PACK_ID"),
                        chcDF("PACK_ID") === prodDF("PACK_ID"),
                        "left"
                    )
                    .drop(chcDF("PACK_ID"))
                    .drop(prodDF("PACK_ID"))
                    // CITY_ID
                    .join(
                        cityDF,
                        chcDF("CITY") === cityDF("NAME"),
                        "left"
                    )
                    .drop(chcDF("CITY"))
                    .drop(cityDF("NAME"))
        }

        Map(
            "chcERD" -> chcERD,
            "dateERD" -> dateDF,
            "prodDIS" -> prodDF
        )
    }

    override def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val chcBaseDF = args.getOrElse("chcBaseDF", throw new Exception("not found hospBaseDF"))
        val revenueDF = args.getOrElse("revenueDF", Seq.empty[String].toDF("_id"))
        val dateDF = args.getOrElse("dateDF", Seq.empty[String].toDF("_id"))
        val cityDF = args.getOrElse("cityDF", Seq.empty[String].toDF("_id"))
        val productDF = args.getOrElse("productDF", Seq.empty[String].toDF("_id"))
        val packDF = args.getOrElse("packDF", Seq.empty[String].toDF("_id"))
        val oadDF = args.getOrElse("oadDF", Seq.empty[String].toDF("_id"))
        val moleDF = args.getOrElse("moleDF", Seq.empty[String].toDF("_id"))
        val manufactureDF = args.getOrElse("manufactureDF", Seq.empty[String].toDF("_id"))
        val chcDF = chcBaseDF.drop("_id").join(productDF, chcBaseDF("prod_id") === productDF("_id"), "left").drop("_id", "prod_id")
                .join(packDF, col("pack_id") === col("_id"), "left").drop("_id", "pack_id")
                .join(moleDF, col("mole_id") === col("_id"), "left").drop("_id", "mole_id")
                .join(manufactureDF, col("manufacturer_id") === col("_id"), "left").drop("_id", "manufacturer_id")
                .join(oadDF, col("oad_id") === col("_id"), "left").drop("_id", "oad_id")
                .join(revenueDF, col("revenue_id") === col("_id"), "left").drop("_id", "revenue_id")
                .join(dateDF, col("date_id") === col("_id"), "left").drop("_id", "date_id")
                .join(cityDF, col("city_id") === col("_id"), "left").drop("_id", "city_id")
        Map("chcDF" -> chcDF)
    }
}
