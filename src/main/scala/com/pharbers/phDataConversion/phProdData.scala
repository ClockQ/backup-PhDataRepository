package com.pharbers.phDataConversion

import org.bson.types.ObjectId
import com.pharbers.spark.phSparkDriver
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import com.pharbers.common.{phDataTrait, phFactory}
import com.pharbers.model.prodData
import org.apache.spark.sql.types.{StringType, StructType}


class phProdData() extends Serializable with phDataTrait {
    def getDataFromDF(df: DataFrame): Unit ={
        val dist_df = df
            .na.fill("")
            .distinct()

        val rddTemp = dist_df.toJavaRDD.rdd.map(x => prodData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
            x(5).toString, x(6).toString, x(7).toString, x(8).toString))

        val refData = rddTemp.groupBy(x => x.productName + x.value + x.standardUnit).flatMap(x => {
            val productID = getObjectID()
            x._2.map(y => {
                y.productID = productID
                y
            })
        }).groupBy(x => x.moleName).flatMap(x => {
            val moleID = getObjectID()
            x._2.map(y => {
                y.moleID = moleID
                y
            })
        }).groupBy(x => x.packageDes + x.packageNumber).flatMap(x => {
            val packageID = getObjectID()
            x._2.map(y => {
                y.packageID = packageID
                y
            })
        }).groupBy(x => x.dosage).flatMap(x => {
            val dosageID = getObjectID()
            x._2.map(y => {
                y.dosageID = dosageID
                y
            })
        }).groupBy(x => x.deliveryWay).flatMap(x => {
            val deliveryID = getObjectID()
            x._2.map(y => {
                y.deliveryID = deliveryID
                y
            })
        }).groupBy(x => x.corpName).flatMap(x => {
            val corpID = getObjectID()
            x._2.map(y => {
                y.corpID = corpID
                y
            })
        })

        saveProd(refData)
        saveMole(refData)
        savePackage(refData)
        saveDosage(refData)
        saveDelivery(refData)
        saveCorp(refData)

    }

    private def saveProd(data: RDD[prodData]): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val rdd = data.map(x => {
            (x.productID, x.productName, x.value, x.standardUnit, x.moleID, x.packageID, x.dosageID, x.deliveryID, x.corpID)
        }).distinct
        phDataHandFunc.saveParquet(rdd.toDF("_id", "product-name", "value", "standard-unit", "mole-id", "package-id", "dosage-id", "delivery-id", "corp-id"),
            "/test/prod/", "prod")
    }

    private def saveMole(data: RDD[prodData]): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val rdd = data.map(x => {
            (x.moleID, x.moleName)
        }).distinct
        phDataHandFunc.saveParquet(rdd.toDF("_id", "mole-name"), "/test/prod/", "mole")
    }

    private def savePackage(data: RDD[prodData]): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val rdd = data.map(x => {
            (x.packageID, x.packageDes, x.packageNumber)
        }).distinct
        phDataHandFunc.saveParquet(rdd.toDF("_id", "package-des", "package-number"), "/test/prod/", "package")
    }

    private def saveDosage(data: RDD[prodData]): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._
        val rdd = data.map(x => {
            (x.dosageID, x.dosage)
        }).distinct
        phDataHandFunc.saveParquet(rdd.toDF("_id", "dosage"), "/test/prod/", "dosage")
    }

    private def saveDelivery(data: RDD[prodData]): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._
        val rdd = data.map(x => {
            (x.deliveryID, x.deliveryWay)
        }).distinct
        phDataHandFunc.saveParquet(rdd.toDF("_id", "delivery-way"), "/test/prod/", "delivery")
        //直接在源数据上加ObjectId成功例子
//        val rdd = data.map(x => {
//            Row(new GenericRowWithSchema(Array(x.deliveryID), new StructType()
//                .add("_id", new StructType()
//                    .add("oid", StringType))), x.deliveryWay)
//        }).distinct
//        val schema = new StructType()
//            .add("_id", new StructType()
//                .add("oid", StringType))
//            .add("delivery-way", StringType)
//        val testDF = sparkDriver.ss.createDataFrame(rdd, schema)
//        phDataHandFunc.saveParquet(testDF, "/test/prod/", "delivery")
    }

    private def saveCorp(data: RDD[prodData]): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val rdd = data.map(x => {
            (x.corpID, x.corpName)
        }).distinct
        phDataHandFunc.saveParquet(rdd.toDF("_id", "corp-name"), "/test/prod/", "corp")
    }

    def getObjectID(): String ={
        ObjectId.get().toHexString
    }

}
