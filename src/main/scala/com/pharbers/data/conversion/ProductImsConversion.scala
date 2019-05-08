package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame
import com.pharbers.data.util.commonUDF
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * @description: product of IMS
  * @author: clock
  * @date: 2019-04-15 14:47
  */
case class ProductImsConversion() extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    override def toERD(args: MapArgs): MapArgs = {
        val columnSeq = Seq(
            "IMS_SOURCE", "IMS_PACK_ID", "IMS_PRODUCT_NAME", "IMS_CORP_NAME", "IMS_MOLE_NAME",
            "IMS_PACKAGE_DES", "IMS_PACKAGE_NUMBER", "IMS_DOSAGE_NAME", "IMS_DELIVERY_WAY"
        )

        val prodBaseDF = args.get.getOrElse("prodBaseDF", throw new Exception("not found prodBaseDF")).getBy[DFArgs]
                .select($"Prd_desc".as("PRD_DESC"), // 1. IMS_PRODUCT_NAME
                    $"Pack_Id".as("IMS_PACK_ID"), // 2. IMS_MOLE_NAME & 8. IMS_PACK_ID
                    $"Str_Desc".as("STR_DESC"), // 3. IMS_PACKAGE_DES
                    $"PckVol_Desc".as("PCKVOL_DESC"), // 3. IMS_PACKAGE_DES
                    $"PckSize_Desc".as("IMS_PACKAGE_NUMBER"), // 4. IMS_PACKAGE_NUMBER
                    $"MNF_ID".as("MNF_ID"), // 5. IMS_CORP_NAME
                    $"Pck_Desc".as("PCK_DESC"), // 7. IMS_DOSAGE_NAME
                    $"NFC123_Code".as("IMS_DELIVERY_WAY"), // 6. IMS_DELIVERY_WAY
                    $"PckSize_Desc".as("PCKSIZE_DESC") // 7. IMS_DOSAGE_NAME
                ).distinct() // 112848

        val mnfDF = args.get.getOrElse("mnfDF", throw new Exception("not found mnfDF")).getBy[DFArgs]
                .select($"MNF_ID", $"Mnf_Desc".as("IMS_CORP_NAME")) // 5. IMS_CORP_NAME
                .distinct() // 6762

        val lkpDF = args.get.getOrElse("lkpDF", throw new Exception("not found lkpDF")).getBy[DFArgs]
                .select($"Pack_ID".as("PACK_ID"), $"Molecule_ID".as("MOLE_ID")) // 2. IMS_MOLE_NAME
                .distinct() // 147152

        val molDF = args.get.getOrElse("molDF", throw new Exception("not found molDF")).getBy[DFArgs]
                .select($"Molecule_Id".as("MOLE_ID"), $"Molecule_Desc".as("IMS_MOLE_NAME")) // 2. IMS_MOLE_NAME
                .distinct() // 20328

        val splitProdMnf: UserDefinedFunction = udf { str: String =>
            str.split(" ").dropRight(1).mkString(" ")
        }

        val splitDosagePackage: UserDefinedFunction = udf { (originStr: String, elemStr: String) =>
            originStr.split(elemStr).head
        }

        val packAndMoleDF = lkpDF.join(molDF, lkpDF("MOLE_ID") === molDF("MOLE_ID")).drop(molDF("MOLE_ID"))
                .groupBy("PACK_ID")
                .agg(sort_array(collect_list("IMS_MOLE_NAME")) as "IMS_MOLE_NAME")
                .withColumn("IMS_MOLE_NAME", commonUDF.mkStringUdf($"IMS_MOLE_NAME", lit("+")))

        val productImsERD = {
            prodBaseDF
                    .addColumn("IMS_SOURCE", "CHC")
                    // 1. IMS_PRODUCT_NAME
                    .withColumn("IMS_PRODUCT_NAME", splitProdMnf(prodBaseDF("PRD_DESC")))
                    // 2. IMS_MOLE_NAME
                    .join(packAndMoleDF, prodBaseDF("IMS_PACK_ID") === packAndMoleDF("PACK_ID"))
                    .drop(packAndMoleDF("PACK_ID"))
                    // 3. IMS_PACKAGE_DES
                    .withColumn("IMS_PACKAGE_DES", concat(prodBaseDF("STR_DESC"), prodBaseDF("PCKVOL_DESC")))
                    // 5. IMS_CORP_NAME
                    .join(mnfDF, prodBaseDF("MNF_ID") === mnfDF("MNF_ID"))
                    .drop("MNF_ID")
                    // 7. IMS_DOSAGE_NAME
                    .withColumn("IMS_DOSAGE_NAME",
                when(col("STR_DESC") === "",
                    when(col("PCKVOL_DESC") === "",
                        splitDosagePackage(col("PCK_DESC"), col("PCKSIZE_DESC"))
                    ).otherwise(splitDosagePackage(col("PCK_DESC"), col("PCKVOL_DESC")))
                ).otherwise(splitDosagePackage(col("PCK_DESC"), col("STR_DESC")))
            )
                    // Adjust the order
                    .select(columnSeq.head, columnSeq.tail: _*)
                    .generateId
        }

        MapArgs(Map(
            "productImsERD" -> DFArgs(productImsERD)
        ))
    }

    override def toDIS(args: MapArgs): MapArgs = {
        val productImsERD = args.get.getOrElse("productImsERD", throw new Exception("not found productImsERD")).getBy[DFArgs]
        val atc3ERD = args.get.getOrElse("atc3ERD", DFArgs(Seq.empty[(String, String, String)].toDF("_id", "PACK_ID", "ATC3"))).getBy[DFArgs]
        val oadERD = args.get.getOrElse("oadERD", DFArgs(Seq.empty[(String, String, String)].toDF("_id", "ATC3", "OAD_TYPE"))).getBy[DFArgs]
        val productDevERD = args.get.getOrElse("productDevERD", DFArgs(Seq.empty[(String, String)].toDF("_id", "DEV_PACK_ID"))).getBy[DFArgs]

        val productImsDIS = {
            productImsERD
                    .withColumn("IMS_PRODUCT_ID", $"_id")
                    .join(
                        atc3ERD.dropDuplicates("PACK_ID")
                        , productImsERD("IMS_PACK_ID").cast("string") === atc3ERD("PACK_ID").cast("string")
                        , "left"
                    )
                    .drop(atc3ERD("_id"))
                    .drop(atc3ERD("PACK_ID"))
                    .join(
                        oadERD.dropDuplicates("ATC3")
                        , atc3ERD("ATC3") === oadERD("ATC3")
                        , "left"
                    )
                    .drop(oadERD("_id"))
                    .drop(oadERD("ATC3"))
                    .join(
                        productDevERD.withColumnRenamed("_id", "DEV_PRODUCT_ID").dropDuplicates("DEV_PACK_ID")
                        , productImsERD("IMS_PACK_ID") === productDevERD("DEV_PACK_ID")
                        , "left"
                    )
        }

        MapArgs(Map("productImsDIS" -> DFArgs(productImsDIS)))
    }
}
