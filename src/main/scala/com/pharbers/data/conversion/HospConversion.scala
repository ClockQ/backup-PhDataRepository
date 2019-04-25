package com.pharbers.data.conversion

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class HospConversion() extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    override def toERD(args: MapArgs): MapArgs = ???

    override def toDIS(args: MapArgs): MapArgs = {
        val hospBaseERD = args.get.getOrElse("hospBaseERD", throw new Exception("not found hospBaseERD")).getBy[DFArgs]
        val hospBedERD = args.get.getOrElse("hospBedERD", Seq.empty[String].toDF("_id"))
        val hospEstimateERD = args.get.getOrElse("hospEstimateERD", Seq.empty[String].toDF("_id"))
        val hospOutpatientERD = args.get.getOrElse("hospOutpatientERD", Seq.empty[String].toDF("_id"))
        val hospRevenueERD = args.get.getOrElse("hospRevenueERD", Seq.empty[String].toDF("_id"))
        val hospSpecialtyERD = args.get.getOrElse("hospSpecialtyERD", Seq.empty[String].toDF("_id"))
        val hospStaffNumERD = args.get.getOrElse("hospStaffNumERD", Seq.empty[String].toDF("_id"))
        val hospUnitERD = args.get.getOrElse("hospUnitERD", Seq.empty[String].toDF("_id"))
        val hospAddressERD = args.get.getOrElse("hospAddressERD", DFArgs(Seq.empty[String].toDF("_id"))).getBy[DFArgs]
        val hospPrefectureERD = args.get.getOrElse("hospPrefectureERD", DFArgs(Seq.empty[String].toDF("_id"))).getBy[DFArgs]
        val hospCityERD = args.get.getOrElse("hospCityERD", DFArgs(Seq.empty[String].toDF("_id"))).getBy[DFArgs]
        val hospProvinceERD = args.get.getOrElse("hospProvinceERD", DFArgs(Seq.empty[String].toDF("_id"))).getBy[DFArgs]

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

        MapArgs(Map("hospDIS" -> DFArgs(hospDIS)))
    }
}
