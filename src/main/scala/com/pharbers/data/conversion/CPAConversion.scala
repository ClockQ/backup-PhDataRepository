package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class CPAConversion() extends PhDataConversion {

    def DF2ERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val hospDF = args.getOrElse("hospDF", throw new Exception("not found hospDF"))
        val prodDF = args.getOrElse("prodDF", throw new Exception("not found prodDF"))



        Map(
            "cpaDF" -> ???,
            "revenueDF" -> ???
        )
    }

    def ERD2DF(args: Map[String, DataFrame]): Map[String, DataFrame] = ???
}
