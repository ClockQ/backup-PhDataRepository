package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

case class GYCConversion(company: String, source_id: String)
        extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val gycDF = args.getOrElse("gycDF", throw new Exception("not found gycDF"))
        val hospDF = args.getOrElse("hospDF", throw new Exception("not found hospDF"))
        val prodDF = args.getOrElse("prodDF", throw new Exception("not found prodDF"))
        val phaDF = args.getOrElse("phaDF", throw new Exception("not found phaDF"))

        val gycERD = gycDF
                .str2Time
                .withColumn("source-id", lit(source_id))
                .join(
                    phaDF.drop("_id").dropDuplicates("GYC")
                    , gycDF("HOSP_ID") === phaDF("GYC")
                    , "left"
                )
                .join(
                    hospDF.withColumnRenamed("_id", "hosp-id").dropDuplicates("PHAHospId")
                    , phaDF("PHA_ID_NEW") === hospDF("PHAHospId")
                    , "left"
                )
                .join(
                    prodDF.withColumnRenamed("_id", "product-id")
                    , gycDF("PRODUCT_NAME") === prodDF("product-name")
                            && gycDF("MOLE_NAME") === prodDF("mole-name")
                            && gycDF("DOSAGE") === prodDF("dosage")
                            && gycDF("PACK_DES") === prodDF("package-des")
                            && gycDF("PACK_NUMBER") === prodDF("package-number")
                            && gycDF("CORP_NAME") === prodDF("corp-name")
                    , "left"
                )
                .generateId
                .select("_id", "source-id", "time", "hosp-id", "product-id", "VALUE", "STANDARD_UNIT")


        Map(
            "gycERD" -> gycERD
        )
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val gycERD = args.getOrElse("gycERD", throw new Exception("not found gycERD"))
        val hospERD = args.getOrElse("hospERD", throw new Exception("not found hospERD"))
        val prodERD = args.getOrElse("prodERD", throw new Exception("not found prodERD"))
        val phaERD = args.getOrElse("phaERD", throw new Exception("not found phaERD"))

        val gycDIS = ???

        Map(
            "gycDIS" -> gycDIS
        )
    }
}
