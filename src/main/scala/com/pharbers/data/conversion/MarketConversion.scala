package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

/**
  * @description:
  * @author: clock
  * @date: 2019-04-18 17:09
  */
case class MarketConversion() extends PhDataConversion {

    import com.pharbers.data.util.sparkDriver.ss.implicits._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val prodDIS = args.getOrElse("prodDIS", throw new Exception("not found prodDIS"))
        println(prodDIS.dropDuplicates("PRODUCT_ID").count())
        val marketTableDF = args.getOrElse("marketTableDF", throw new Exception("not found marketTableDF"))

        val marketDF = prodDIS
                .join(
                    marketTableDF,
                    prodDIS("PH_MOLE_NAME") === marketTableDF("MOLE_NAME"),
                    "left"
                )
                .select($"PRODUCT_ID", $"COMPANY_ID", $"MARKET")
                .distinct()
        println(prodDIS.count())
        println(marketTableDF.count())
        println(marketDF.count())
        Map(
            "marketDF" -> marketDF
        )
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = ???
}
