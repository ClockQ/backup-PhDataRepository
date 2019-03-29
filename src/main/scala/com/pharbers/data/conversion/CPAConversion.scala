package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class CPAConversion() extends PhDataConversion {

    def DF2ERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val cpaDF = args.getOrElse("cpaDF", throw new Exception("not found cpaDF"))
        val hospDF = args.getOrElse("hospDF", throw new Exception("not found hospDF"))
        val prodDF = args.getOrElse("prodDF", throw new Exception("not found prodDF"))

        val prod_with_min1_df = prodDF
                .select("product-id", "product-name", "package-des", "package-number", "dosage", "corp-name")

        Map(
            "cpaDF" -> ???,
            "revenueDF" -> ???
        )
    }

    def ERD2DF(args: Map[String, DataFrame]): Map[String, DataFrame] = ???
}
