package com.pharbers.data.conversion

import com.pharbers.model.prodData
import com.pharbers.phDataConversion.phDataHandFunc
import org.apache.spark.sql.DataFrame

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class ProdConversion() extends PhDataConversion {

    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {

        val sourceDataDF = args.getOrElse("sourceDataDF", throw new Exception("not found sourceDataDF"))

        val dist_df = sourceDataDF
            .select("PRODUCT_NAME", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME")
            .na.fill("")
            .distinct()

        val rddTemp = dist_df.toJavaRDD.rdd.map(x => prodData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
            x(5).toString, x(6).toString))

        val refData = rddTemp.groupBy(x => x.moleName).flatMap(x => {
            val moleID = phDataHandFunc.getObjectID()
            x._2.map(y => {
                y.moleID = moleID
                y
            })
        }).groupBy(x => x.packageDes + x.packageNumber).flatMap(x => {
            val packageID = phDataHandFunc.getObjectID()
            x._2.map(y => {
                y.packageID = packageID
                y
            })
        }).groupBy(x => x.dosage).flatMap(x => {
            val dosageID = phDataHandFunc.getObjectID()
            x._2.map(y => {
                y.dosageID = dosageID
                y
            })
        }).groupBy(x => x.deliveryWay).flatMap(x => {
            val deliveryID = phDataHandFunc.getObjectID()
            x._2.map(y => {
                y.deliveryID = deliveryID
                y
            })
        }).groupBy(x => x.corpName).flatMap(x => {
            val corpID = phDataHandFunc.getObjectID()
            x._2.map(y => {
                y.corpID = corpID
                y
            })
        }).cache()

        val prodERD = refData.map(x => {
            (phDataHandFunc.getObjectID(), x.productName, x.moleID, x.packageID, x.dosageID, x.deliveryID, x.corpID)
        }).cache().distinct.toDF("_id", "product-name", "mole-id", "package-id", "dosage-id", "delivery-id", "corp-id")

        val prodMoleERD = refData.map(x => {
            (x.moleID, x.moleName)
        }).distinct.toDF("_id", "mole-name")

        val prodPackageERD = refData.map(x => {
            (x.packageID, x.packageDes, x.packageNumber)
        }).distinct.toDF("_id", "package-des", "package-number")

        val prodDosageERD = refData.map(x => {
            (x.dosageID, x.dosage)
        }).distinct.toDF("_id", "dosage")

        val prodDeliveryERD = refData.map(x => {
            (x.deliveryID, x.deliveryWay)
        }).distinct.toDF("_id", "delivery-way")

        val prodCorpERD = refData.map(x => {
            (x.corpID, x.corpName)
        }).distinct.toDF("_id", "corp-name")

        Map(
            "prodERD" -> prodERD,
            "prodDeliveryERD" -> prodDeliveryERD,
            "prodDosageERD" -> prodDosageERD,
            "prodMoleERD" -> prodMoleERD,
            "prodPackageERD" -> prodPackageERD,
            "prodCorpERD" -> prodCorpERD
        )
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val prodBaseERD = args.getOrElse("prodBaseERD", throw new Exception("not found prodBaseDF"))
        val prodDeliveryERD = args.getOrElse("prodDeliveryERD", Seq.empty[String].toDF("_id"))
        val prodDosageERD = args.getOrElse("prodDosageERD", Seq.empty[String].toDF("_id"))
        val prodMoleERD = args.getOrElse("prodMoleERD", Seq.empty[String].toDF("_id"))
        val prodPackageERD = args.getOrElse("prodPackageERD", Seq.empty[String].toDF("_id"))
        val prodCorpERD = args.getOrElse("prodCorpERD", Seq.empty[String].toDF("_id"))

        val prodDIS = prodBaseERD
                .join(
                    prodMoleERD.withColumnRenamed("_id", "main-id"),
                    col("mole-id") === col("main-id"),
                    "left"
                ).drop(col("main-id"))
                .join(
                    prodDeliveryERD.withColumnRenamed("_id", "main-id"),
                    col("delivery-id") === col("main-id"),
                    "left"
                ).drop(col("main-id"))
                .join(
                    prodDosageERD.withColumnRenamed("_id", "main-id"),
                    col("dosage-id") === col("main-id"),
                    "left"
                ).drop(col("main-id"))
                .join(
                    prodPackageERD.withColumnRenamed("_id", "main-id"),
                    col("package-id") === col("main-id"),
                    "left"
                ).drop(col("main-id"))
                .join(
                    prodCorpERD.withColumnRenamed("_id", "main-id"),
                    col("corp-id") === col("main-id"),
                    "left"
                ).drop(col("main-id"))

        Map(
            "prodDIS" -> prodDIS
        )
    }
}
