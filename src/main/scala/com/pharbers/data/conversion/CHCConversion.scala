package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs}

case class CHCConversion() extends PhDataConversion2 {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    def toCHCStruct(dis: DataFrame): DataFrame = dis.select(
        $"IMS_PACK_ID".as("PACK_ID"), $"TIME", $"CITY_NAME".as("CITY")
        , $"DEV_PRODUCT_NAME", $"DEV_MOLE_NAME", $"DEV_CORP_NAME"
        , $"DEV_PACKAGE_NUMBER", $"DEV_DOSAGE_NAME", $"DEV_PACKAGE_DES"
        , $"ATC3", $"OAD_TYPE"
        , $"SALES", $"UNITS"
    )

    override def toERD(args: MapArgs): MapArgs = {
        val chcDF = args.get.getOrElse("chcDF", throw new Exception("not found chcDF")).getBy[DFArgs]
        val dateDF = args.get.getOrElse("dateDF", throw new Exception("not found dateDF")).getBy[DFArgs]
        val productDIS = args.get.getOrElse("productDIS", throw new Exception("not found productDIS")).getBy[DFArgs]
//                .filter($"DEV_PRODUCT_NAME".isNull)
                .dropDuplicates("IMS_PACK_ID")
                .select($"IMS_PRODUCT_ID".as("PRODUCT_ID"), $"IMS_PACK_ID")
        val cityDF = args.get.getOrElse("cityDF", throw new Exception("not found cityDF")).getBy[DFArgs]
                .select($"_id".as("CITY_ID"), regexp_replace($"name", "市", "").as("NAME"))
                .dropDuplicates("NAME")
        val addCHCProdFunc = args.get.get("addCHCProdFunc")

        val chcERD = {
            chcDF
                    .join(// DATE_ID
                        dateDF.withColumnRenamed("_id", "DATE_ID"),
                        chcDF("Date") === dateDF("TIME"),
                        "left"
                    )
                    .join( // PRODUCT_ID
                        productDIS,
                        chcDF("Pack_ID") === productDIS("IMS_PACK_ID"),
                        "left"
                    )
                    .join( // CITY_ID
                        cityDF,
                        chcDF("city") === cityDF("NAME"),
                        "left"
                    ) // Adjust the order
                    .select($"PRODUCT_ID", $"CITY_ID", $"DATE_ID", $"Sales".as("SALES"), $"Units".as("UNITS"))
                    .generateId
        }

//        val funcedERD = addCHCProdFunc match {
//            case Some(funcArgs) =>
//                val func = funcArgs.getBy[SingleArgFuncArgs[DataFrame, DataFrame]]
//                func(chcERD)
//            case None =>
//        }

        MapArgs(Map("chcERD" -> DFArgs(chcERD)))
    }

    override def toDIS(args: MapArgs): MapArgs = {
        val chcERD = args.get.getOrElse("chcERD", throw new Exception("not found chcERD")).getBy[DFArgs]
        val productDIS = args.get.getOrElse("productDIS", throw new Exception("not found productDIS")).getBy[DFArgs]
        val cityERD = args.get.getOrElse("cityERD", throw new Exception("not found cityERD")).getBy[DFArgs]
        val dateERD = args.get.getOrElse("dateERD", throw new Exception("not found dateERD")).getBy[DFArgs]

        val addressDIS = AddressConversion().toDIS(MapArgs(Map(
            "cityERD" -> DFArgs(cityERD)
        ))).getAs[DFArgs]("addressDIS")

        val chcDIS = {
            chcERD
                    .join(
                        productDIS,
                        chcERD("PRODUCT_ID") === productDIS("IMS_PRODUCT_ID"),
                        "left"
                    ).drop(productDIS("_id"))
                    .join(
                        dateERD,
                        chcERD("DATE_ID") === dateERD("_id"),
                        "left"
                    ).drop(dateERD("_id"))
                    .join(
                        addressDIS,
                        chcERD("CITY_ID") === addressDIS("CITY_ID"),
                        "left"
                    ).drop(addressDIS("CITY_ID"))
        }

        MapArgs(Map("chcDIS" -> DFArgs(chcDIS)))
    }
}
