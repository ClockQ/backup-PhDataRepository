package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

/**
  * @description: product of calc
  * @author: clock
  * @date: 2019-04-15 14:49
  */
case class ProductDevConversion()(imsCvs: ProductImsConversion, etcCvs: ProductEtcConversion) extends PhDataConversion {

    import com.pharbers.data.util.DFUtil
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    def matchTable2Product(df: DataFrame): DataFrame = df
            .trim("DELIVERY_WAY")
            .select(
                $"STANDARD_PRODUCT_NAME" as "PRODUCT_NAME"
                , $"STANDARD_MOLE_NAME" as "MOLE_NAME"
                , $"STANDARD_PACK_DES" as "PACKAGE_DES"
                , $"PACK_NUMBER" as "PACKAGE_NUMBER"
                , $"STANDARD_CORP_NAME" as "CORP_NAME"
                , $"DELIVERY_WAY"
                , $"STANDARD_DOSAGE" as "DOSAGE_NAME"
                , $"PACK_ID"
            )

    def chc2Product(df: DataFrame): DataFrame = df
            .trim("DELIVERY_WAY")
            .trim("PACKAGE_DES")
            .trim("PACKAGE_NUMBER")
            .trim("DOSAGE_NAME")
            .select(
                $"Prod_Desc" as "PRODUCT_NAME"
                , $"Molecule_Desc" as "MOLE_NAME"
                , $"PACKAGE_DES"
                , $"PACKAGE_NUMBER"
                , $"MNF_Desc" as "CORP_NAME"
                , $"DELIVERY_WAY"
                , $"DOSAGE_NAME"
                , $"Pack_ID" as "PACK_ID"
            )

    override def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val productDevERD = args.values.reduce(_ unionByName _)
                .dropDuplicates("PRODUCT_NAME", "MOLE_NAME", "PACKAGE_DES",
                    "PACKAGE_NUMBER", "CORP_NAME", "DELIVERY_WAY", "DOSAGE_NAME", "PACK_ID")
                .generateId
        Map(
            "productDevERD" -> productDevERD
        )
    }

    override def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val productDevERD = args.getOrElse("productDevERD", throw new Exception("not found productDevERD"))
        val productImsERD = args.get("productImsERD")
        val productEtcERD = args.get("productEtcERD")

        val prodConnImsERD = productImsERD match {
            case Some(_) =>
                val imsERD = imsCvs.toDIS(args)("productImsDIS")
                productDevERD.join(imsERD, productDevERD("PACK_ID") === imsERD("IMS_PACK_ID"), "left")
                        .drop(imsERD("_id")).drop(imsERD("IMS_PACK_ID"))
            case None => productDevERD
        }

        val productDIS = productEtcERD match {
            case Some(_) =>
                val etcERD = etcCvs.toDIS(args)("productEtcDIS")
                prodConnImsERD.join(etcERD, productDevERD("_id") === etcERD("PRODUCT_ID"), "right")
                        .drop(etcERD("_id")).drop(etcERD("PRODUCT_ID"))
            case None => prodConnImsERD
        }

        Map(
            "productDIS" -> productDIS
        )
    }
}