package com.pharbers

import com.pharbers.common.{phFactory, savePath2Mongo}
import com.pharbers.phDataConversion.{phDataHandFunc, phHospData, phProdData, phRegionData}
import com.pharbers.spark.util.readParquet
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object main extends App{
    val driver = phFactory.getSparkInstance()
    import driver.conn_instance
    driver.sc.addJar("target/pharbers-data-repository-1.0-SNAPSHOT.jar")

    val pfizer_cpa = driver.ss.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load("/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv")
        .select("PRODUCT_NAME", "VALUE", "STANDARD_UNIT", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME")
        .cache()
    val pfizer_gyc = driver.ss.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load("/test/CPA&GYCX/Pfizer_201804_Gycx_20181127.csv")
        .select("PRODUCT_NAME", "VALUE", "STANDARD_UNIT", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME")
        .cache()
    val astellas_cpa = driver.ss.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load("/test/CPA&GYCX/Astellas_201804_CPA_20180629.csv")
        .select("PRODUCT_NAME", "VALUE", "STANDARD_UNIT", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME")
        .cache()
    val astellas_gyc = driver.ss.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load("/test/CPA&GYCX/Astellas_201804_Gycx_20180703.csv")
        .select("PRODUCT_NAME", "VALUE", "STANDARD_UNIT", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME")
        .cache()
    val nhwa_cpa = driver.ss.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load("/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv")
        .select("PRODUCT_NAME", "VALUE", "STANDARD_UNIT", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME")
        .cache()

    var df = pfizer_cpa union pfizer_gyc union astellas_cpa union astellas_gyc union nhwa_cpa
    new phProdData().getDataFromDF(df)

    new savePath2Mongo().saveDF("/test/prod")
    println("ALL DONE")

//    driver.sc.addJar("D:\\code\\pharbers\\phDataRepository new\\target\\pharbers-data-repository-1.0-SNAPSHOT.jar")
//    driver.sc.addJar("C:\\Users\\EDZ\\.m2\\repository\\com\\pharbers\\spark_driver\\1.0\\spark_driver-1.0.jar")
//    driver.sc.addJar("C:\\Users\\EDZ\\.m2\\repository\\org\\mongodb\\mongo-java-driver\\3.8.0\\mongo-java-driver-3.8.0.jar")
//    var df = driver.ss.read.format("com.databricks.spark.csv")
//            .option("header", "true")
//            .option("delimiter", ",")
//            .load("/test/2019年Universe更新维护1.0.csv")
//            .withColumn("addressId", phDataHandFunc.setIdCol())
//            .cache()
//
//    df.columns.foreach(x => {
//        df = df.withColumnRenamed(x, x.trim)
//    })
//
//    new phHospData().getHospDataFromCsv(df)
//    new phRegionData().getRegionDataFromCsv(df)
//
//
////    var dfMap: Map[String, DataFrame] = Map("hosp" -> null,"outpatient" -> null,"bed" -> null,"revenue" -> null,"staff" -> null,"specialty" -> null, "estimate" -> null)
////
////    dfMap = dfMap.map(x => {
////        (x._1, driver.setUtil(readParquet()).readParquet("/test/hosp/" + x._1))
////    })
////
////    math(dfMap)
//
//
//    def math(dfMap: Map[String, DataFrame]): Unit ={
//        var refString = ""
//        val cityRDD = dfMap("city").select("tier", "name").toJavaRDD.rdd.map(x => (x(0).asInstanceOf[Seq[String]], x(1).toString))
//                .filter(x => x._1.length > 1).map(x => (x._1(1), x._2))
//
//        val tier2010RDD = dfMap("tier").filter(col("tag") === "2018").select("_id", "tier").toJavaRDD.rdd.map(x => (x(0).toString, x(1).toString))
//        val tier = cityRDD.join(tier2010RDD).map(x => (x._2._2,1)).reduceByKey((left, right) => left + right)
//        tier.collect().foreach(x => refString = refString + x._1+ "级城市有" + x._2 + "个\n")
//
////        val addressRDD = dfMap("address").select("region", "_id").toJavaRDD.rdd.map(x => (x(0).asInstanceOf[Seq[String]].head, x(1).toString))
////        val regionRDD = dfMap("region").select("_id", "name").toJavaRDD.rdd.map(x => (x(0).toString, x(1).toString))
////
////        val region = addressRDD.join(regionRDD).map(x => (x._2._2 ,1)).reduceByKey((left, right) => left + right)
////        region.collect().foreach(x => refString = refString + x._1 + "有" + x._2  + "个" + "地址\n")
//        println(refString)
//    }
//
//    case class tableInfo(name: String, path: String, outKey: String, key: String, isHasTag: Boolean)
//    def readTable(inputData: DataFrame, tag: String, outputData: DataFrame, inputInfo: tableInfo, outputInfo: tableInfo)
//                 (tagFunc: (String, DataFrame) => DataFrame)(filterFunc:(DataFrame, tableInfo,DataFrame, tableInfo) => DataFrame): DataFrame ={
//        filterFunc(inputData, inputInfo,tagFunc(tag, outputData), outputInfo)
//    }
}


