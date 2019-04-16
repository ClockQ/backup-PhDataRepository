package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * @description: product of IMS
  * @author: clock
  * @date: 2019-04-15 14:47
  */
case class IMSProductConversion() extends PhDataConversion {

    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val columnSeq = Seq(
            "IMS_PRODUCT_NAME", "IMS_MOLE_NAME", "IMS_PACKAGE_DES", "IMS_PACKAGE_NUMBER"
            , "IMS_CORP_NAME", "IMS_DELIVERY_WAY", "IMS_DOSAGE_NAME", "IMS_PACK_ID"
        )

        val prodBaseDF = args.getOrElse("prodBaseDF", throw new Exception("not found prodBaseDF"))
                .select($"Prd_desc".as("PRD_DESC"), // 1. IMS_PRODUCT_NAME
                    $"Pack_Id".as("IMS_PACK_ID"), // 2. IMS_MOLE_NAME & 8. IMS_PACK_ID
                    $"Str_Desc".as("STR_DESC"), // 3. IMS_PACKAGE_DES
                    $"PckVol_Desc".as("PCKVOL_DESC"), // 3. IMS_PACKAGE_DES
                    $"PckSize_Desc".as("IMS_PACKAGE_NUMBER"), // 4. IMS_PACKAGE_NUMBER
                    $"MNF_ID".as("MNF_ID"), // 5. IMS_CORP_NAME
                    $"Pck_Desc".as("PCK_DESC"), // 7. IMS_DOSAGE_NAME
                    $"PckSize_Desc".as("PCKSIZE_DESC") // 7. IMS_DOSAGE_NAME
                ).distinct() // 112848

        val mnfDF = args.getOrElse("mnfDF", throw new Exception("not found mnfDF"))
                .select($"MNF_ID", $"Mnf_Desc".as("IMS_CORP_NAME")) // 5. IMS_CORP_NAME
                .distinct() // 6762

        val lkpDF = args.getOrElse("lkpDF", throw new Exception("not found lkpDF"))
                .select($"Pack_ID".as("PACK_ID"), $"Molecule_ID".as("MOLE_ID")) // 2. IMS_MOLE_NAME
                .distinct() // 147152

        val molDF = args.getOrElse("molDF", throw new Exception("not found molDF"))
                .select($"Molecule_Id".as("MOLE_ID"), $"Molecule_Desc".as("IMS_MOLE_NAME")) // 2. IMS_MOLE_NAME
                .distinct() // 20328

        val splitProdMnf: UserDefinedFunction = udf { str: String =>
            str.split(" ").dropRight(1).mkString(" ")
        }

        val splitDosagePackage: UserDefinedFunction = udf { (originStr: String, elemStr: String) =>
            originStr.split(elemStr).head
        }

        val packAndMoleDF = lkpDF.join(molDF, lkpDF("MOLE_ID") === molDF("MOLE_ID")).drop(molDF("MOLE_ID"))//.filter(col("PACK_ID") === "202")
                        .groupBy("MOLE_ID")
                        .agg(concat_ws("IMS_MOLE_NAME"))

        packAndMoleDF.show(false)

        val ImsERD = {
            prodBaseDF
                    // 1. IMS_PRODUCT_NAME
                    .withColumn("IMS_PRODUCT_NAME", splitProdMnf(prodBaseDF("PRD_DESC")))
                    // 2. IMS_MOLE_NAME
                    .join(lkpDF, prodBaseDF("IMS_PACK_ID") === lkpDF("PACK_ID"))
                    .drop(lkpDF("PACK_ID"))
                    .join(molDF, lkpDF("MOLE_ID") === molDF("MOLE_ID"))
                    .drop("MOLE_ID")
                    // 3. IMS_PACKAGE_DES
                    .withColumn("IMS_PACKAGE_DES", concat(prodBaseDF("STR_DESC"), prodBaseDF("PCKVOL_DESC")))
                    // 5. IMS_CORP_NAME
                    .join(mnfDF, prodBaseDF("MNF_ID") === mnfDF("MNF_ID"))
                    .drop("MNF_ID")
                    // 6. IMS_DELIVERY_WAY
                    .withColumn("IMS_DELIVERY_WAY", lit(""))
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
        }

        Map(
            "ImsERD" -> ImsERD
        )
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = ???

}
