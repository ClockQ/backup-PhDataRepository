package com.pharbers.data.conversion

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

/**
  * @description: product of calc
  * @author: clock
  * @date: 2019-04-15 14:49
  */
case class ProductDevConversion() extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    override def toERD(args: MapArgs): MapArgs = {
        val productDevERD = args.get.values.map(_.getBy[DFArgs]).reduce(_ unionByName _)
                .groupBy("DEV_PRODUCT_NAME", "DEV_CORP_NAME", "DEV_MOLE_NAME",
                    "DEV_PACKAGE_DES", "DEV_PACKAGE_NUMBER", "DEV_DELIVERY_WAY", "DEV_DOSAGE_NAME")
                .agg(max("DEV_PACK_ID") as "DEV_PACK_ID")
                .generateId

        MapArgs(Map(
            "productDevERD" -> DFArgs(productDevERD)
        ))
    }

    override def toDIS(args: MapArgs): MapArgs = {
        val productDevERD = args.get.getOrElse("productDevERD", throw new Exception("not found productDevERD")).getBy[DFArgs]
        val productImsERD = args.get.get("productImsERD")
        val productEtcERD = args.get.get("productEtcERD")
//
//        val prodConnImsERD = productImsERD match {
//            case Some(_) =>
//                val imsERD = imsCvs.toDIS(args).getAs[DFArgs]("productImsDIS")
//                productDevERD.join(imsERD, productDevERD("PACK_ID") === imsERD("IMS_PACK_ID"), "left")
//                        .drop(imsERD("_id")).drop(imsERD("IMS_PACK_ID"))
//            case None => productDevERD
//        }
//
//        val productDevDIS = productEtcERD match {
//            case Some(_) =>
//                val etcERD = etcCvs.toDIS(args).getAs[DFArgs]("productEtcDIS")
//                prodConnImsERD.join(etcERD, productDevERD("_id") === etcERD("PRODUCT_ID"), "right")
//                        .drop(etcERD("_id")).drop(etcERD("PRODUCT_ID"))
//            case None => prodConnImsERD
//        }
        val productDevDIS = ???

        MapArgs(Map(
            "productDevDIS" -> DFArgs(productDevDIS)
        ))
    }
}