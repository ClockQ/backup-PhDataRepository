package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame
import com.pharbers.phDataConversion.phDataHandFunc

/**
  * @description: product of pharbers
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class ProductEtcConversion(company_id: String) extends PhDataConversion {

    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {

        val sourceDataDF = args.getOrElse("sourceDataDF", throw new Exception("not found sourceDataDF"))

        val prodERD = sourceDataDF
            .select("PRODUCT_NAME", "MOLE_NAME", "PACK_DES", "PACK_NUMBER", "DOSAGE", "DELIVERY_WAY", "CORP_NAME")
            .distinct()
            .withColumn("COMPANY_ID", lit(company_id))
            .na.fill("")

        Map(
            "prodERD" -> prodERD
        )
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val prodERD = args.getOrElse("prodERD", throw new Exception("not found prodERD"))

        //TODO:目前prod维度均为一层
        val prodDIS = prodERD

        Map(
            "prodDIS" -> prodDIS
        )
    }
}
