package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}
import com.pharbers.spark.phSparkDriver

case class MaxResultConversion()(implicit val sparkDriver: phSparkDriver) extends PhDataConversion {

    import com.pharbers.data.util._
    import sparkDriver.ss.implicits._
    import org.apache.spark.sql.functions._

    override def toERD(args: MapArgs): MapArgs = {
        val maxDF = args.get.getOrElse("maxDF", throw new Exception("not found maxDF")).getBy[DFArgs]
        val hospDF = args.get.getOrElse("hospDF", throw new Exception("not found hospDF")).getBy[DFArgs]
        val prodDF = {
            args.get.getOrElse("prodDF", throw new Exception("not found prodDF")).getBy[DFArgs]
                    .withColumn("MIN2", concat(
                        col("DEV_PRODUCT_NAME"),
                        col("DEV_DOSAGE_NAME"),
                        col("DEV_PACKAGE_DES"),
                        col("DEV_PACKAGE_NUMBER"),
                        col("DEV_CORP_NAME"))
                    ).dropDuplicates("MIN2")
        }

        val maxERD = maxDF.join(
            prodDF
            , maxDF("Product") === prodDF("MIN2")
            , "left"
        ).join(
            hospDF.dropDuplicates("PHA_HOSP_ID")
            , maxDF("Panel_ID") === hospDF("PHA_HOSP_ID")
            , "left"
        ).select(
            $"COMPANY_ID"
            , $"Date" as "YM"
            , hospDF("_id") as "HOSPITAL_ID"
            , prodDF("_id") as "PRODUCT_ID"
            , $"MARKET"
            , $"Factor"
            , $"belong2company"
            , $"f_sales" as "SALES"
            , $"f_units" as "UNITS"
        ).generateId

        MapArgs(Map("maxERD" -> DFArgs(maxERD)))
    }

    override def toDIS(args: MapArgs): MapArgs = {
        val maxERD = args.get.getOrElse("maxERD", throw new Exception("not found maxERD")).getBy[DFArgs]
        val prodDIS = args.get.getOrElse("prodDIS", throw new Exception("not found prodDIS")).getBy[DFArgs]
        val hospDIS = args.get.getOrElse("hospDIS", throw new Exception("not found hospDIS")).getBy[DFArgs]

        val maxDIS = {
            maxERD
                    .join(
                        hospDIS
                        , maxERD("HOSPITAL_ID") === hospDIS("_id")
                        , "left"
                    )
                    .drop(hospDIS("_id"))
                    .join(
                        prodDIS
                        , maxERD("PRODUCT_ID") === prodDIS("_id")
                        , "left"
                    )
                    .drop(prodDIS("_id"))
        }

        MapArgs(Map("maxDIS" -> DFArgs(maxDIS)))
    }
}
