package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

case class MaxResultConversion(company_id: String) extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val maxDF = args.getOrElse("maxDF", throw new Exception("not found maxDF"))
        //Date, Province, City, Panel_ID, Product, Factor, f_sales, f_units, MARKET

        val sourceERD = maxDF
            .select("MARKET")
            .distinct()
            .withColumn("COMPANY_ID", lit(company_id))
            .generateId
            .cache()

        val maxERD = maxDF
            .distinct()
            .generateId
            .join(sourceERD
                .withColumnRenamed("MARKET", "mkt")
                .withColumnRenamed("_id", "SOURCE_ID"),
                col("MARKET") === col("mkt"),
                "left")
            .withColumnRenamed("Date", "YM")
            .str2Time
            .withColumnRenamed("Panel_ID", "PHA_ID")
            .withColumnRenamed("Product", "MIN_PRODUCT")
            .withColumnRenamed("Factor", "FACTOR")
            .withColumnRenamed("f_sales", "SALES")
            .withColumnRenamed("f_units", "UNITS")
            .select("_ID", "SOURCE_ID", "TIME", "PHA_ID", "MIN_PRODUCT", "FACTOR", "SALES", "UNITS")

        Map(
            "maxERD" -> maxERD,
            "sourceERD" -> sourceERD
        )
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val maxERD = args.getOrElse("maxERD", throw new Exception("not found maxERD"))
        val hospDIS = args.getOrElse("hospDIS", throw new Exception("not found hospDIS"))
        val prodDIS = args.getOrElse("prodDIS", throw new Exception("not found prodDIS"))

        // TODO:匹配医院已完成对数，目前MaxResultHospDIS只使用了[PHAHospId / City / Province]，原因是HospDIS数据中有PHAHospId一对多的问题，
        // TODO:匹配产品已完成对数，但是max结果数据中的min1是规范产品名等的数据，再使用min1反匹到我们prodDIS【来源于CPA等未经规范的原始数据】，就会出现很多匹配不到的情况。等待公司维度的匹配表的数据仓储的建立。
//        val hospDistinct = hospDIS.filter(col("PHAIsRepeat") === 0).select("PHAHospId", "prefecture-name", "city-name", "province-name").distinct()
//            .groupBy("PHAHospId").agg(("PHAHospId" -> "count"))

        val maxDIS = maxERD
            .join(
                hospDIS.filter(col("PHAIsRepeat") === 0).select("PHAHospId", "city-name", "province-name").distinct(),
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
