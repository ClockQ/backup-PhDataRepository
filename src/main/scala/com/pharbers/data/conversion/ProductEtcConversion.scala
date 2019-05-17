package com.pharbers.data.conversion

import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs, StringArgs}

/**
  * @description: product of pharbers
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class ProductEtcConversion()(implicit val sparkDriver: phSparkDriver) extends PhDataConversion {

    import com.pharbers.data.util._
    import sparkDriver.ss.implicits._
    import org.apache.spark.sql.functions._

    override def toERD(args: MapArgs): MapArgs = {
        val company_id = args.getAs[StringArgs]("company_id")
        val matchMarketFunc = args.getAs[SingleArgFuncArgs[MapArgs, MapArgs]]("matchMarketFunc")
        val matchDevFunc = args.getAs[SingleArgFuncArgs[MapArgs, MapArgs]]("matchDevFunc")

        val matchMarketProdDF = matchMarketFunc(args).get.head._2.getBy[DFArgs]

        val matchDevProdDF = matchDevFunc(
            MapArgs(args.get + ("prodMatchDF" -> DFArgs(matchMarketProdDF)))
        ).get.head._2.getBy[DFArgs]

        val productEtcERD = matchDevProdDF
                .select(
                    lit(company_id) as "ETC_COMPANY_ID"
                    , $"STANDARD_PRODUCT_NAME" as "ETC_PRODUCT_NAME"
                    , $"STANDARD_CORP_NAME" as "ETC_CORP_NAME"
                    , $"STANDARD_MOLE_NAME" as "ETC_MOLE_NAME"
                    , $"STANDARD_PACK_DES" as "ETC_PACKAGE_DES"
                    , $"PACK_COUNT" as "ETC_PACKAGE_NUMBER"
                    , $"STANDARD_DOSAGE" as "ETC_DOSAGE_NAME"
                    , lit("") as "ETC_DELIVERY_WAY"
                    , $"MARKET"
                    , $"DEV_PRODUCT_ID"
                )
                .distinct()
                .generateId

        MapArgs(Map(
            "productEtcERD" -> DFArgs(productEtcERD)
        ))
    }

    override def toDIS(args: MapArgs): MapArgs = {
        val productEtcERD = args.get.getOrElse("productEtcERD", throw new Exception("not found productEtcERD"))
                .getBy[DFArgs].withColumnRenamed("_id", "ETC_PRODUCT_ID")
        val atcERD = args.get.get("atcERD")
        val productDevERD = args.get.get("productDevERD")

        val etcConnAtcDF = atcERD match {
            case Some(atc) =>
                val atcDF = atc.getBy[DFArgs].distinctByKey("MOLE_NAME")()
                productEtcERD
                        .join(
                            atcDF
                            , col("ETC_MOLE_NAME") === atcDF("MOLE_NAME")
                            , "left"
                        )
                        .drop(atcDF("_id"))
                        .drop(atcDF("MOLE_NAME"))
            case None => productEtcERD
        }

        val etcConnDevDF = productDevERD match {
            case Some(dev) =>
                val devDF = dev.getBy[DFArgs]
                etcConnAtcDF
                        .join(
                            devDF
                            , col("DEV_PRODUCT_ID") === devDF("_id")
                            , "left"
                        )
                        .drop(devDF("_id"))
            case None => etcConnAtcDF
        }

        MapArgs(Map("productEtcDIS" -> DFArgs(etcConnDevDF)))
    }

    def matchDevFunc(args: MapArgs): MapArgs = {
        val prodMatchDF = args.getAs[DFArgs]("prodMatchDF")
        val prodDevDF = args.getAs[DFArgs]("prodDevDF").withColumn("DEV_PRODUCT_ID", $"_id")
        val resultDF = prodMatchDF.join(
            prodDevDF
            , prodMatchDF("STANDARD_PRODUCT_NAME") === prodDevDF("DEV_PRODUCT_NAME")
                    && prodMatchDF("STANDARD_MOLE_NAME") === prodDevDF("DEV_MOLE_NAME")
                    && prodMatchDF("STANDARD_DOSAGE") === prodDevDF("DEV_DOSAGE_NAME")
                    && prodMatchDF("STANDARD_PACK_DES") === prodDevDF("DEV_PACKAGE_DES")
                    && prodMatchDF("PACK_COUNT") === prodDevDF("DEV_PACKAGE_NUMBER")
                    && prodMatchDF("STANDARD_CORP_NAME") === prodDevDF("DEV_CORP_NAME")
            , "left"
        )

        val nullCount = resultDF.filter($"DEV_PRODUCT_ID".isNull).count()
        if (nullCount != 0)
            throw new Exception("product exist " + nullCount + " null `DEV_PRODUCT_ID`")

        MapArgs(Map("result" -> DFArgs(resultDF)))
    }
}
