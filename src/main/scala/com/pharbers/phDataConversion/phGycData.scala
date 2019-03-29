package com.pharbers.phDataConversion

import com.pharbers.common.{phDataTrait, phFactory}
import com.pharbers.model.gycData
import com.pharbers.spark.phSparkDriver
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame


class phGycData() extends Serializable with phDataTrait {
    def getDataFromDF(df: DataFrame): Unit ={
        val dist_df = df
            .select("hospital-id", "product-id", "VALUE", "STANDARD_UNIT", "YM", "SOURCE", "TAG")
            .na.fill("")
            .distinct()

        val rddTemp = dist_df.toJavaRDD.rdd.map(x => gycData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
            x(5).toString, x(6).toString))

        val refData = rddTemp.groupBy(x => x.hospitalID + x.productID + x.ym + x.source + x.tag).flatMap(x => {
            val gycID = phDataHandFunc.getObjectID()
            x._2.map(y => {
                y.gycID = gycID
                y
            })
        }).groupBy(x => x.ym + x.value + x.unit).flatMap(x => {
            val saleInfoID = phDataHandFunc.getObjectID()
            x._2.map(y => {
                y.saleInfoID = saleInfoID
                y
            })
        }).groupBy(x => x.source + x.tag).flatMap(x => {
            val sourceID = phDataHandFunc.getObjectID()
            x._2.map(y => {
                y.sourceID = sourceID
                y
            })
        }).cache()

        saveGyc(refData)
        saveSource(refData)
        saveSaleInfo(refData)

    }

    private def saveGyc(data: RDD[gycData]): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val rdd = data.map(x => {
            (x.gycID, x.hospitalID, x.productID, x.sourceID, x.saleInfoID)
        }).distinct
        phDataHandFunc.saveParquet(rdd.toDF("_id", "hospital-id", "product-id", "source-id", "saleInfo-id"),
            "/repository/", "gyc")
    }

    private def saveSource(data: RDD[gycData]): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val rdd = data.map(x => {
            (x.sourceID, x.source, x.tag)
        }).distinct
        phDataHandFunc.saveParquet(rdd.toDF("_id", "title", "tag"),
            "/repository/", "source")
    }

    private def saveSaleInfo(data: RDD[gycData]): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val rdd = data.map(x => {
            (x.saleInfoID, x.value, x.unit, x.ym)
        }).distinct
        phDataHandFunc.saveParquet(rdd.toDF("_id", "value", "unit", "ym"),
            "/repository/", "saleInfo")
    }

}
