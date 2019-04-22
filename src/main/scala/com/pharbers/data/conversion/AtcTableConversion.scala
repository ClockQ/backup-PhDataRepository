package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class AtcTableConversion() extends PhDataConversion {

    import com.pharbers.data.util.DFUtil
    import org.apache.spark.sql.functions._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val atcTableDF = args.map(x => x._2.trim("ATC_CODE").select("MOLE_NAME", "ATC_CODE"))
                .reduce(_ unionByName _)
                .filter(col("ATC_CODE") =!= "")
                .distinct()
                .generateId

        Map(
            "atcTableDF" -> atcTableDF
        )
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = ???
}
