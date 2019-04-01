package com.pharbers

import com.pharbers.common.{phFactory, savePath2Mongo}
import com.pharbers.phDataConversion._
import com.pharbers.spark.util.readParquet
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


object main extends App{
    val driver = phFactory.getSparkInstance()
    import driver.ss.implicits._
    import driver.conn_instance
    driver.sc.addJar("target/pharbers-data-repository-1.0-SNAPSHOT.jar")

    val pfizer_cpa = driver.ss.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load("/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv").withColumn("MONTH", 'MONTH.cast(IntegerType))
        .withColumn("MONTH", when(col("MONTH").>=(10), col("MONTH"))
            .otherwise(concat(col("MONTH").*(0).cast("int"), col("MONTH"))))
        .withColumn("YM", concat(col("YEAR"), col("MONTH")))
        .select("HOSP_ID", "PRODUCT_NAME", "VALUE", "STANDARD_UNIT", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME", "YM")
        .withColumn("SOURCE", lit("Pfizer"))
        .withColumn("TAG", lit("CPA"))
        .cache()
    val pfizer_gyc = driver.ss.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load("/test/CPA&GYCX/Pfizer_201804_Gycx_20181127.csv")
        .withColumn("MONTH", when(col("MONTH").>=(10), col("MONTH"))
            .otherwise(concat(col("MONTH").*(0).cast("int"), col("MONTH"))))
        .withColumn("YM", concat(col("YEAR"), col("MONTH")))
        .select("HOSP_ID", "PRODUCT_NAME", "VALUE", "STANDARD_UNIT", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME", "YM")
        .withColumn("SOURCE", lit("Pfizer"))
        .withColumn("TAG", lit("GYC"))
        .cache()
    val astellas_cpa = driver.ss.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load("/test/CPA&GYCX/Astellas_201804_CPA_20180629.csv")
        .select("HOSP_ID", "PRODUCT_NAME", "VALUE", "STANDARD_UNIT", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME", "YM")
        .withColumn("SOURCE", lit("Astellas"))
        .withColumn("TAG", lit("CPA"))
        .cache()
    val astellas_gyc = driver.ss.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load("/test/CPA&GYCX/Astellas_201804_Gycx_20180703.csv")
        .select("HOSP_ID", "PRODUCT_NAME", "VALUE", "STANDARD_UNIT", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME", "YM")
        .withColumn("SOURCE", lit("Astellas"))
        .withColumn("TAG", lit("GYC"))
        .cache()
    val nhwa_cpa = driver.ss.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load("/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv")
        .withColumn("MONTH", when(col("MONTH").>=(10), col("MONTH"))
            .otherwise(concat(col("MONTH").*(0).cast("int"), col("MONTH"))))
        .withColumn("YM", concat(col("YEAR"), col("MONTH")))
        .select("HOSP_ID", "PRODUCT_NAME", "VALUE", "STANDARD_UNIT", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME", "YM")
        .withColumn("SOURCE", lit("Nhwa"))
        .withColumn("TAG", lit("CPA"))
        .cache()

    val cpa_df = (pfizer_cpa union astellas_cpa union nhwa_cpa)
        .na.fill("")
        .distinct()
    val gyc_df = (pfizer_gyc union astellas_gyc)
        .na.fill("")
        .distinct()
    val origin_df = cpa_df union gyc_df
    //TODO:prod层的，待抽离整合
    new phProdData().getDataFromDF(origin_df)

    val cpa_gyc_pha = driver.ss.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load("/test/CPA&GYCX/CPA_GYC_PHA.csv")
        .na.fill("-")
        .cache()
    val hosp_df = driver.setUtil(readParquet()).readParquet("/test/hosp/hosp").select("PHAHospId", "_id")
    //TODO:根据产品特征匹配已存的prod,仓库添加新的源数据CPA或GYC时一定会有以前的产品,重新把prod再生成一次是愚蠢的,最好把以前的prod加载进来,按照min1遍历查询,存在就使用原来的id,不存在就新加.
    //TODO:把以前的prod加载进来,需要把min1五项属性[PRODUCT_NAME+DOSAGE+PACKAGE_DES+PACKAGE_NUMBER+CORP_NAME],把三个关联表 dosage+package+corp 关联到prod上
    val prod_df = driver.setUtil(readParquet()).readParquet("/repository/prod").select("_id", "product-name", "package-id", "dosage-id", "corp-id")
        .withColumnRenamed("_id", "product-id")
    val dosage_df = driver.setUtil(readParquet()).readParquet("/repository/dosage")
    val package_df = driver.setUtil(readParquet()).readParquet("/repository/package")
    val corp_df = driver.setUtil(readParquet()).readParquet("/repository/corp")
    val prod_with_min1_df = prod_df
        .join(dosage_df, prod_df("dosage-id") === dosage_df("_id"), "left").drop(dosage_df("_id"))
        .join(package_df, prod_df("package-id") === package_df("_id"), "left").drop(package_df("_id"))
        .join(corp_df, prod_df("corp-id") === corp_df("_id"), "left").drop(corp_df("_id"))
        .select("product-id", "product-name", "package-des", "package-number", "dosage", "corp-name")

    val gyc_df_with_pha = gyc_df.join(cpa_gyc_pha, gyc_df("HOSP_ID") === cpa_gyc_pha("GYC"), "left")
        .na.fill("")
        .distinct()
    val gyc_df_with_hosp_oid = gyc_df_with_pha.join(hosp_df, gyc_df_with_pha("PHA_ID_NEW") === hosp_df("PHAHospId"), "left")
        .na.fill("")
        .withColumnRenamed("_id", "hospital-id")
        .select("hospital-id", "PRODUCT_NAME", "VALUE", "STANDARD_UNIT", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME", "SOURCE", "TAG", "YM")

    val gyc_df_with_hosp_prod_oid = gyc_df_with_hosp_oid
        .join(prod_with_min1_df,
            gyc_df_with_hosp_oid("PRODUCT_NAME") === prod_with_min1_df("product-name") &&
                gyc_df_with_hosp_oid("DOSAGE") === prod_with_min1_df("dosage") &&
                gyc_df_with_hosp_oid("PACK_DES") === prod_with_min1_df("package-des") &&
                gyc_df_with_hosp_oid("PACK_NUMBER") === prod_with_min1_df("package-number") &&
                gyc_df_with_hosp_oid("CORP_NAME") === prod_with_min1_df("corp-name"),
            "left")
        .select("hospital-id", "product-id", "VALUE", "STANDARD_UNIT", "SOURCE", "TAG", "YM")

    //TODO:gyc层的，待抽离整合
    new phGycData().getDataFromDF(gyc_df_with_hosp_prod_oid)

    //TODO:save2mongo层的，待抽离整合
    new savePath2Mongo().saveDF("/repository")
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


