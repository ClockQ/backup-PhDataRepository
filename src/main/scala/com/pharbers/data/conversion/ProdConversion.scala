package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class ProdConversion() extends PhDataConversion {

    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = ???

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val prodBaseDF = args.getOrElse("prodBaseDF", throw new Exception("not found prodBaseDF"))
        val prodDeliveryDF = args.getOrElse("prodDeliveryDF", Seq.empty[String].toDF("_id"))
        val prodDosageDF = args.getOrElse("prodDosageDF", Seq.empty[String].toDF("_id"))
        val prodMoleDF = args.getOrElse("prodMoleDF", Seq.empty[String].toDF("_id"))
        val prodPackageDF = args.getOrElse("prodPackageDF", Seq.empty[String].toDF("_id"))
        val prodCorpDF = args.getOrElse("prodCorpDF", Seq.empty[String].toDF("_id"))

        val prodDF = prodBaseDF
                .join(
                    prodMoleDF.withColumnRenamed("_id", "main-id"),
                    col("mole-id") === col("main-id"),
                    "left"
                ).drop(col("main-id"))
                .join(
                    prodDeliveryDF.withColumnRenamed("_id", "main-id"),
                    col("delivery-id") === col("main-id"),
                    "left"
                ).drop(col("main-id"))
                .join(
                    prodDosageDF.withColumnRenamed("_id", "main-id"),
                    col("dosage-id") === col("main-id"),
                    "left"
                ).drop(col("main-id"))
                .join(
                    prodPackageDF.withColumnRenamed("_id", "main-id"),
                    col("package-id") === col("main-id"),
                    "left"
                ).drop(col("main-id"))
                .join(
                    prodCorpDF.withColumnRenamed("_id", "main-id"),
                    col("corp-id") === col("main-id"),
                    "left"
                ).drop(col("main-id"))

        Map(
            "prodDF" -> prodDF
        )
    }
}
