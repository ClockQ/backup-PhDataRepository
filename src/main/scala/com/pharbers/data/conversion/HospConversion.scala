package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class HospConversion() extends PhDataConversion {

    import com.pharbers.data.util.sparkDriver.ss.implicits._
    import org.apache.spark.sql.functions._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = ???

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val hospBaseERD = args.getOrElse("hospBaseERD", throw new Exception("not found hospBaseERD"))
        val hospBedERD = args.getOrElse("hospBedERD", Seq.empty[String].toDF("_id"))
        val hospEstimateERD = args.getOrElse("hospEstimateERD", Seq.empty[String].toDF("_id"))
        val hospOutpatientERD = args.getOrElse("hospOutpatientERD", Seq.empty[String].toDF("_id"))
        val hospRevenueERD = args.getOrElse("hospRevenueERD", Seq.empty[String].toDF("_id"))
        val hospSpecialtyERD = args.getOrElse("hospSpecialtyERD", Seq.empty[String].toDF("_id"))
        val hospStaffNumERD = args.getOrElse("hospStaffNumERD", Seq.empty[String].toDF("_id"))
        val hospUnitERD = args.getOrElse("hospUnitERD", Seq.empty[String].toDF("_id"))
        val hospAddressERD = args.getOrElse("hospAddressERD", Seq.empty[String].toDF("_id"))
        val hospPrefectureERD = args.getOrElse("hospPrefectureERD", Seq.empty[String].toDF("_id"))
        val hospCityERD = args.getOrElse("hospCityERD", Seq.empty[String].toDF("_id"))
        val hospProvinceERD = args.getOrElse("hospProvinceERD", Seq.empty[String].toDF("_id"))

        val hospDIS = hospBaseERD
            .join(
                hospAddressERD.withColumnRenamed("_id", "main-id"),
                col("addressID") === col("main-id"),
                "left"
            ).drop(col("main-id"))
            .join(
                hospPrefectureERD
                    .withColumnRenamed("_id", "main-id")
                    .withColumnRenamed("name", "prefecture-name")
                    .drop("polygon"),
                col("prefecture") === col("main-id"),
                "left"
            ).drop(col("main-id"))
            .join(
                hospCityERD
                    .withColumnRenamed("_id", "main-id")
                    .withColumnRenamed("name", "city-name")
                    .drop("polygon"),
                col("city") === col("main-id"),
                "left"
            ).drop(col("main-id"))
            .join(
                hospProvinceERD
                    .withColumnRenamed("_id", "main-id")
                    .withColumnRenamed("name", "province-name")
                    .drop("polygon"),
                col("province") === col("main-id"),
                "left"
            ).drop(col("main-id"))

        Map(
            "hospDIS" -> hospDIS
        )
    }
}
