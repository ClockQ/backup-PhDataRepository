package com.pharbers.run

import com.pharbers.common.phFactory
import com.pharbers.phDataConversion.{phDataHandFunc, phHospData, phRegionData}

/**
  * @description:
  * @author: clock
  * @date: 2019-04-16 19:24
  */
object TransformHosp extends App {
    val driver = phFactory.getSparkInstance()
    driver.sc.addJar("target/pharbers-data-repository-1.0-SNAPSHOT.jar")
    var df = driver.ss.read.format("com.databricks.spark.csv")
            .option("header", "true")
            .option("delimiter", ",")
            .load("/test/2019年Universe更新维护1.0.csv")
            .withColumn("addressId", phDataHandFunc.setIdCol())
            .cache()

    df.columns.foreach(x => {
        df = df.withColumnRenamed(x, x.trim)
    })

    new phHospData().getHospDataFromCsv(df)
    new phRegionData().getRegionDataFromCsv(df)
}
