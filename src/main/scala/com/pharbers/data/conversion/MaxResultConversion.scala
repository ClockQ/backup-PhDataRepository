package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

case class MaxResultConversion(company_id: String) extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val maxDF = args.getOrElse("maxDF", throw new Exception("not found maxDF"))
        //Date, Province, City, Panel_ID, Product, Factor, f_sales, f_units, MARKET
        val maxERD = maxDF
            .generateId
            .withColumn("SOURCE_ID", lit(company_id))
            .withColumnRenamed("Date", "YM")
            .str2Time
            .withColumnRenamed("Panel_ID", "PHA_ID")
            .withColumnRenamed("Product", "MIN_PRODUCT")
            .withColumnRenamed("Factor", "FACTOR")
            .withColumnRenamed("f_sales", "SALES")
            .withColumnRenamed("f_units", "UNITS")
            .select("_ID", "SOURCE_ID", "TIME", "PHA_ID", "MIN_PRODUCT", "FACTOR", "SALES", "UNITS", "MARKET")

        Map(
            "maxERD" -> maxERD
        )
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val maxERD = args.getOrElse("maxERD", throw new Exception("not found maxERD"))
        val hospDIS = args.getOrElse("hospDIS", throw new Exception("not found hospDIS"))
        val prodDIS = args.getOrElse("prodDIS", throw new Exception("not found prodDIS"))

        //TODO:HospDIS数据有重复的PHAHospId,导致一对多的问题，产品已完成匹配与对数

        val maxDIS = maxERD
            .join(
                hospDIS.withColumnRenamed("_id", "HOSPITAL_ID"),
                col("PHA_ID") === col("PHAHospId"),
                "left"
            ).drop(col("PHAHospId"))
            .join(
                prodDIS
                    .withColumnRenamed("_id", "PRODUCT_ID")
                    //TODO:统一命名 -> 【全大写，单词间下划线相连。】
//                    .withColumn("min1", concat(col("PRODUCT_NAME"), col("DOSAGE"), col("PACK_DES"), col("PACK_NUMBER"), col("CORP_NAME"))),
                    .withColumn("min1", concat(col("product-name"), col("dosage"), col("package-des"), col("package-number"), col("corp-name"))),
                col("MIN_PRODUCT") === col("min1"),
                "left"
            ).drop(col("min1"))
            .time2ym

        Map(
            "maxDIS" -> maxDIS
        )
    }
}
