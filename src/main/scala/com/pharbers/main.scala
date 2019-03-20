package com.pharbers

import com.pharbers.common.phFactory
import com.pharbers.phDataConversion.phRegionData
import com.pharbers.spark.util.readParquet
import org.apache.spark.sql.functions._


object main extends App{
    val driver = phFactory.getSparkInstance()
    import driver.conn_instance
    driver.sc.addJar("D:\\code\\pharbers\\phDataRepository new\\target\\pharbers-data-repository-1.0-SNAPSHOT.jar")
    driver.sc.addJar("C:\\Users\\EDZ\\.m2\\repository\\com\\pharbers\\spark_driver\\1.0\\spark_driver-1.0.jar")
    driver.sc.addJar("C:\\Users\\EDZ\\.m2\\repository\\org\\mongodb\\mongo-java-driver\\3.8.0\\mongo-java-driver-3.8.0.jar")
    val df = driver.ss.read.format("com.databricks.spark.csv")
            .option("header", "true")
            .option("delimiter", ",")
            .load("/test/2019年Universe更新维护1.0.csv")
//    val df2 = driver.setUtil(readParquet()).readParquet("/test/testAddress/city")

    new phRegionData().getRegionDataFromCsv(df)

}
