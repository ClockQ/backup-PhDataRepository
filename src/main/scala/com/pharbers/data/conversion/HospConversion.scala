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

        val hospDIS = hospBaseERD

        Map(
            "hospDIS" -> hospDIS
        )
    }
}
