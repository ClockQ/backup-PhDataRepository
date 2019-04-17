package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class OadAndAtc3TableConversion() extends PhDataConversion {

    import com.pharbers.data.util.DFUtil
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val chcDF = args.getOrElse("chcDF", throw new Exception("not found chcDF"))

        val atc3AndOadTable = chcDF
                .select($"Pack_ID".as("PACK_ID"), $"ATC3", $"OAD类别".as("OAD_TYPE"))

        val atc3ERD = atc3AndOadTable.select("PACK_ID", "ATC3").distinct().generateId

        val oadERD = atc3AndOadTable.select("ATC3", "OAD_TYPE").distinct().generateId

        Map(
            "atc3ERD" -> atc3ERD
            , "oadERD" -> oadERD
        )
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = ???
}
