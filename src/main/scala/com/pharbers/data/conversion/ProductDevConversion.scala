package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

/**
  * @description: product of calc
  * @author: clock
  * @date: 2019-04-15 14:49
  */
case class ProductDevConversion() extends PhDataConversion {

    import com.pharbers.data.util.DFUtil
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    override def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val productDevERD = {
            args.map(x =>
                x._2.trim("DELIVERY_WAY").select(
                    $"STANDARD_PRODUCT_NAME" as "PRODUCT_NAME"
                    , $"STANDARD_MOLE_NAME" as "MOLE_NAME"
                    , $"STANDARD_PACK_DES" as "PACKAGE_DES"
                    , $"PACK_NUMBER" as "PACKAGE_NUMBER"
                    , $"STANDARD_CORP_NAME" as "CORP_NAME"
                    , $"DELIVERY_WAY"
                    , $"STANDARD_DOSAGE" as "DOSAGE_NAME"
                    , $"PACK_ID"
                ))
                    .reduce(_ union _)
                    .distinct()
                    .generateId
        }

        Map(
            "productDevERD" -> productDevERD
        )
    }

    override def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val productDevERD = args.getOrElse("productDevERD", throw new Exception("not found productDevERD"))
        val productImsERD = args.getOrElse("productImsERD", Seq.empty[(String, String)].toDF("_id", "IMS_PACK_ID"))
        val productEtcERD = args.getOrElse("productEtcERD", Seq.empty[(String, String)].toDF("_id", "PRODUCT_ID"))

        val productDIS = {
            productDevERD
                    .join(productImsERD, productDevERD("PACK_ID") === productImsERD("IMS_PACK_ID"), "left")
                    .drop(productImsERD("_id"))
                    .join(productEtcERD, productDevERD("_id") === productEtcERD("PRODUCT_ID"), "left")
                    .drop(productEtcERD("_id"))
        }

        Map(
            "productDIS" -> productDIS
        )
    }
}
//+---------+------------+------+-----------+----------+--------------------------+--------------------------------------------+------------------+---------------------+-----------------+---------------+------------------+------------------------------------------+-------+-----------------+---------+
//|MOLE_NAME|PRODUCT_NAME|DOSAGE|PACK_DES   |PACK_COUNT|CORP_NAME                 |MIN_PRODUCT_UNIT                            |STANDARD_MOLE_NAME|STANDARD_PRODUCT_NAME|STANDARD_PACK_DES|STANDARD_DOSAGE|STANDARD_CORP_NAME|MIN_PRODUCT_UNIT_STANDARD                 |PACK_ID|PRODUCT_SKU       |WEIGHT_MG|
//+---------+------------+------+-----------+----------+--------------------------+--------------------------------------------+------------------+---------------------+-----------------+---------------+------------------+------------------------------------------+-------+-----------------+---------+
//|咪达唑仑  |力月西       |SOLN  |10 MG 2 ML |1         |江苏恩华药业集团有限公司       |力月西SOLN10 MG 2 ML1江苏恩华药业集团有限公司    |咪达唑仑           |力月西                  |10MG2ML          |注射剂         |江苏恩华药业集团有限公司  |力月西注射剂10MG2ML1江苏恩华药业集团有限公司 |1083504|力月西(江苏恩华)    |10.0     |
//|咪达唑仑  |力月西       |SOLN  |5 MG 1 ML  |1         |江苏恩华药业集团有限公司       |力月西SOLN5 MG 1 ML1江苏恩华药业集团有限公司     |咪达唑仑           |力月西                  |5MG1ML           |注射剂         |江苏恩华药业集团有限公司  |力月西注射剂5MG1ML1江苏恩华药业集团有限公司  |1083506|力月西(江苏恩华)    |5.0      |
