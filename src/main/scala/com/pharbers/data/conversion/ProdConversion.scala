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
