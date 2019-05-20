package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

case class MaxResultConversion(company_id: String) extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val maxDF = args.getOrElse("maxDF", throw new Exception("not found maxDF"))
        //Date, Province, City, Panel_ID, Product, Factor, f_sales, f_units, MARKET

//        val sourceERD = maxDF
//            .select("MARKET")
//            .distinct()
//            .withColumn("COMPANY_ID", lit(company_id))
//            .generateId
//            .cache()

        val sourceERD = args.getOrElse("sourceERD", throw new Exception("not found maxDF"))

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
        val sourceERD = args.getOrElse("sourceERD", throw new Exception("not found sourceERD"))
        val hospDIS = args.getOrElse("hospDIS", throw new Exception("not found hospDIS"))
        val prodDIS = args.getOrElse("prodDIS", throw new Exception("not found prodDIS"))

        // TODO:匹配医院已完成对数，目前MaxResultHospDIS只使用了[PHAHospId / City / Province]，原因是HospDIS数据中有PHAHospId一对多的问题，
        // TODO:匹配产品已完成对数，但是max结果数据中的min_product是规范产品名等的数据，理论上能完全匹配到PH_PROD_DIS中，待测试。
//        val hospDistinct = hospDIS.filter(col("PHAIsRepeat") === 0).select("PHAHospId", "prefecture-name", "city-name", "province-name").distinct()
//            .groupBy("PHAHospId").agg(("PHAHospId" -> "count"))

        val maxDIS = maxERD
            .join(
                sourceERD.withColumnRenamed("_id", "main-id"),
                col("SOURCE_ID") === col("main-id"),
                "left"
            ).drop(col("main-id"))
            .join(
                hospDIS.filter(col("PHAIsRepeat") === 0).select("PHAHospId", "city-name", "province-name", "region-name").distinct(),
                col("PHA_ID") === col("PHAHospId"),
                "left"
            ).drop(col("PHAHospId"))
            .join(
                prodDIS
                    .withColumnRenamed("_id", "PRODUCT_ID")
                    .withColumn("PH_MIN", concat(col("PRODUCT_NAME"), col("DOSAGE_NAME"), col("PACKAGE_DES"), col("PACKAGE_NUMBER"), col("CORP_NAME")))
                    .dropDuplicates("PH_MIN")
                    .drop("COMPANY_ID"),
                col("MIN_PRODUCT") === col("PH_MIN"),
                "left"
            ).drop(col("PH_MIN"))
            .na.fill("")
            .time2ym

        Map(
            "maxDIS" -> maxDIS
        )
    }
}
