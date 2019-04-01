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
        val hospBaseDF = args.getOrElse("hospBaseDF", throw new Exception("not found hospBaseDF"))
        val hospBedDF = args.getOrElse("hospBedDF", Seq.empty[String].toDF("_id"))
        val hospEstimateDF = args.getOrElse("hospEstimateDF", Seq.empty[String].toDF("_id"))
        val hospOutpatientDF = args.getOrElse("hospOutpatientDF", Seq.empty[String].toDF("_id"))
        val hospRevenueDF = args.getOrElse("hospRevenueDF", Seq.empty[String].toDF("_id"))
        val hospSpecialtyDF = args.getOrElse("hospSpecialtyDF", Seq.empty[String].toDF("_id"))
        val hospStaffNumDF = args.getOrElse("hospStaffNumDF", Seq.empty[String].toDF("_id"))
        val hospUnitDF = args.getOrElse("hospUnitDF", Seq.empty[String].toDF("_id"))

        val hospDF = hospBaseDF

        Map(
            "hospDF" -> hospDF
        )
    }
}
