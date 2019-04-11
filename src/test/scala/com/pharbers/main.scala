package com.pharbers

import com.pharbers.common.phFactory
import com.pharbers.phDataConversion.{phDataHandFunc, phGycData, phProdData}
import com.pharbers.spark.util.readParquet
import org.apache.spark.sql.functions.{col, concat, lit, when}
import org.apache.spark.sql.types.IntegerType

/**
  * @description:
  * @author: clock
  * @date: 2019-04-08 19:40
  */
object main extends App{
    val driver = phFactory.getSparkInstance()
    import driver.conn_instance
    import driver.ss.implicits._
    driver.sc.addJar("target/pharbers-data-repository-1.0-SNAPSHOT.jar")

    val cpa_gyc_pha = driver.ss.read.format("com.databricks.spark.csv")
        .option("header", "true")
        .option("delimiter", ",")
        .load("/test/CPA&GYCX/CPA_GYC_PHA.csv")
        .na.fill("")
        .distinct()
        .cache()
    val hosp_df = driver.setUtil(readParquet()).readParquet("/repository/hosp")
    val prod_df = driver.setUtil(readParquet()).readParquet("/repository/prod2")
    val delivery_df = driver.setUtil(readParquet()).readParquet("/repository/delivery2")
    val dosage_df = driver.setUtil(readParquet()).readParquet("/repository/dosage2")
    val mole_df = driver.setUtil(readParquet()).readParquet("/repository/mole2")
    val package_df = driver.setUtil(readParquet()).readParquet("/repository/package2")
    val corp_df = driver.setUtil(readParquet()).readParquet("/repository/corp2")

    phDataHandFunc.saveParquet(cpa_gyc_pha, "/repository/", "5cac311ff789a807b1a8a1f4")
    phDataHandFunc.saveParquet(hosp_df, "/repository/", "5cac3566ceb3c45854b80d28")
    phDataHandFunc.saveParquet(prod_df, "/repository/", "5cac3586ceb3c45854b80d29")
    phDataHandFunc.saveParquet(delivery_df, "/repository/", "5cac359aceb3c45854b80d2a")
    phDataHandFunc.saveParquet(dosage_df, "/repository/", "5cac35adceb3c45854b80d2b")
    phDataHandFunc.saveParquet(mole_df, "/repository/", "5cac35bdceb3c45854b80d2c")
    phDataHandFunc.saveParquet(package_df, "/repository/", "5cac35d2ceb3c45854b80d2d")
    phDataHandFunc.saveParquet(corp_df, "/repository/", "5cac35e5ceb3c45854b80d2e")

    /**
      * val PROD_BASE_LOCATION = "/repository/prod2"
      * val PROD_DELIVERY_LOCATION = "/repository/delivery2"
      * val PROD_DOSAGE_LOCATION = "/repository/dosage2"
      * val PROD_MOLE_LOCATION = "/repository/mole2"
      * val PROD_PACKAGE_LOCATION = "/repository/package2"
      * val PROD_CORP_LOCATION = "/repository/corp2"
      */

    println("ALL DONE")

}
