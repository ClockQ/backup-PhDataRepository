package com.pharbers.data.conversion

import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class HospConversion()(implicit val sparkDriver: phSparkDriver) extends PhDataConversion {

    import com.pharbers.data.util._
    import sparkDriver.ss.implicits._
    import org.apache.spark.sql.functions._

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
        val addressDIS = args.get.getOrElse("addressDIS", DFArgs(Seq.empty[String].toDF("ADDRESS_ID"))).getBy[DFArgs]

        val hospDIS = {
            hospBaseERD
                    .join(
                        addressDIS,
                        hospBaseERD("addressID") === addressDIS("ADDRESS_ID"),
                        "left"
                    ).drop(addressDIS("ADDRESS_ID"))
                    .withColumnRenamed("title", "HOSP_NAME")
                    .withColumnRenamed("PHAIsRepeat", "PHA_IS_REPEAT")
                    .withColumnRenamed("PHAHospId", "PHA_HOSP_ID")
                    .withColumnRenamed("type", "HOSP_TYPE")
                    .withColumnRenamed("level", "HOSP_LEVEL")
                    .withColumnRenamed("character", "HOSP_CHARACTER")
        }

        MapArgs(Map("hospDIS" -> DFArgs(hospDIS)))
    }
}
