package com.pharbers.data.conversion

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class AtcTableConversion() extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._

    override def toERD(args: MapArgs): MapArgs = {
        val atcTableDF = args.get.values.map(_.getBy[DFArgs].addColumn("ATC_CODE").select("MOLE_NAME", "ATC_CODE"))
                .reduce(_ unionByName _)
                .filter(col("ATC_CODE") =!= "")
                .distinct()
                .generateId

        MapArgs(Map("atcTableDF" -> DFArgs(atcTableDF)))
    }

    override def toDIS(args: MapArgs): MapArgs = ???
}
