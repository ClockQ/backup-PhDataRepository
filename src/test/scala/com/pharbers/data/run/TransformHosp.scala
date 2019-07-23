package com.pharbers.data.run

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}
import com.pharbers.data.conversion.hosp.{phDataHandFunc, phFactory, phHospData, phRegionData}

/**
  * @description:
  * @author: clock
  * @date: 2019-04-16 19:24
  */
object TransformHosp extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    import com.pharbers.data.util.spark._
    import sparkDriver.ss.implicits._

    def transformHosp(): Unit = {
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

        // 这里可以看到四条医院实际是一个医院，对于医院这种底层数据，要注意垃圾数据的清理
//        val abc = CSV2DF("/test/2019年Universe更新维护1.0.csv")
//        abc.filter($"新版ID" === "PHA0004258").show(false)
//        Parquet2DF(HOSP_BASE_LOCATION).filter($"PHAHospId" === "PHA0004258").show(false)
    }
//    transformHosp()

    lazy val hospCvs = HospConversion()

    lazy val hospBaseERD = Parquet2DF(HOSP_BASE_LOCATION)
    lazy val hospBaseERDCount = hospBaseERD.count()
    lazy val addressDIS = Parquet2DF(ADDRESS_DIS_LOCATION)
    lazy val addressDISCount = addressDIS.count()

    lazy val hospDIS = hospCvs.toDIS(MapArgs(Map(
        "hospBaseERD" -> DFArgs(hospBaseERD)
        , "addressDIS" -> DFArgs(addressDIS)
    ))).getAs[DFArgs]("hospDIS")
    lazy val hospDISCount = hospDIS.count()
    hospDIS.show(false)

    val hospMinus = hospBaseERDCount - hospDISCount
    assert(hospMinus == 0, "hospital: 转换后的DIS比ERD减少`" + hospMinus + "`条记录")

    if (args.nonEmpty && args(0) == "TRUE")
        hospDIS.save2Parquet(HOSP_DIS_LOCATION)
}
