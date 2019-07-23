package com.pharbers.data.conversion

import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class OadAndAtc3TableConversion()(implicit val sparkDriver: phSparkDriver) extends PhDataConversion {

    import com.pharbers.data.util._
    import sparkDriver.ss.implicits._

    override def toERD(args: MapArgs): MapArgs = {
        val chcDF = args.get.getOrElse("chcDF", throw new Exception("not found chcDF")).getBy[DFArgs]

        val atc3AndOadTable = chcDF.select($"Pack_ID".as("PACK_ID"), $"ATC3", $"OAD类别".as("OAD_TYPE"))

        val atc3ERD = atc3AndOadTable.select("PACK_ID", "ATC3").distinct().generateId

        val oadERD = atc3AndOadTable.select("ATC3", "OAD_TYPE").distinct().generateId

        MapArgs(Map(
            "atc3ERD" -> DFArgs(atc3ERD)
            , "oadERD" -> DFArgs(oadERD)
        ))
    }

    override def toDIS(args: MapArgs): MapArgs = ???
}
