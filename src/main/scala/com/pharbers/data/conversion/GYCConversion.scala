package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame
import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.data.util.commonUDF.generateIdUdf

case class GYCConversion(company_id: String)(prodCvs: ProductEtcConversion) extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val gycDF = args.getOrElse("gycDF", throw new Exception("not found gycDF"))
        val hospDF = args.getOrElse("hospDF", throw new Exception("not found hospDF"))
        val phProdDF = args.getOrElse("prodDF", throw new Exception("not found prodDF"))
        val phaDF = args.getOrElse("phaDF", throw new Exception("not found phaDF"))

        val connProdHosp = {
            gycDF
                    .join(
                        phaDF.drop("_id").dropDuplicates("GYC")
                        , gycDF("HOSP_ID") === phaDF("GYC")
                        , "left"
                    )
                    .join(
                        hospDF.withColumnRenamed("_id", "HOSPITAL_ID").dropDuplicates("PHAHospId")
                        , phaDF("PHA_ID_NEW") === hospDF("PHAHospId")
                        , "left"
                    )
                    .join(
                        phProdDF.withColumnRenamed("_id", "ETC_PRODUCT_ID")
                        , gycDF("PRODUCT_NAME") === phProdDF("ETC_PRODUCT_NAME")
                                && gycDF("MOLE_NAME") === phProdDF("ETC_MOLE_NAME")
                                && gycDF("DOSAGE") === phProdDF("ETC_DOSAGE_NAME")
                                && gycDF("PACK_DES") === phProdDF("ETC_PACKAGE_DES")
                                && gycDF("PACK_NUMBER") === phProdDF("ETC_PACKAGE_NUMBER")
                                && gycDF("CORP_NAME") === phProdDF("ETC_CORP_NAME")
                        , "left"
                    )
//                .drop(prodDF("dosage"))// 同名重复，要删掉
        }

//        // 存在未成功匹配的产品, 递归执行self.toERD
//        val notConnProdOfGyc = connProdHosp.filter(col("PH_PRODUCT_ID").isNull)
//        val notConnProdOfGycCount = notConnProdOfGyc.count()
//        if (notConnProdOfGycCount != 0) {
//            phDebugLog(notConnProdOfGycCount + "条产品未匹配, 重新转换")
//            val notConnProdDIS = prodCvs.toDIS(prodCvs.toERD(Map("sourceDataDF" -> notConnProdOfGyc)))("productEtcDIS")
//            return toERD(args + ("prodDF" -> notConnProdDIS.unionByName(phProdDF)))
//        }
//
//        // 存在未成功匹配的医院, 递归执行self.toERD
//        val notConnHospOfGyc = connProdHosp.filter(col("HOSPITAL_ID").isNull)
//        val notConnHospOfGycCount = notConnHospOfGyc.count()
//        if (notConnHospOfGycCount != 0) {
//            phDebugLog(notConnHospOfGycCount + "条医院未匹配, 重新转换")
//            val notConnPhaDIS = notConnHospOfGyc.select(col("HOSP_ID"))
//                    .distinct()
//                    .withColumnRenamed("HOSP_ID", "GYC")
//                    .withColumn("PHA_ID_NEW", generateIdUdf())
//                    .cache()
//            val notConnHospDIS = notConnPhaDIS.select("PHA_ID_NEW")
//                    .withColumnRenamed("PHA_ID_NEW", "PHAHospId")
//                    .generateId
//            return toERD(args +
//                    ("hospDF" -> hospDF.unionByName(notConnHospDIS.alignAt(hospDF))) +
//                    ("phaDF" -> phaDF.unionByName(notConnPhaDIS.alignAt(phaDF)))
//            )
//        }

        val gycERD = connProdHosp
                .generateId
                .withColumn("COMPANY_ID", lit(company_id))
                .str2Time
                .select($"_id", $"COMPANY_ID", $"YM",
                    $"HOSPITAL_ID".as("HOSP_ID"), $"ETC_PRODUCT_ID".as("PRODUCT_ID"),
                    $"VALUE".as("SALES"), $"STANDARD_UNIT".as("UNITS"))

        Map(
            "gycERD" -> gycERD,
            "prodDIS" -> phProdDF,
            "hospDIS" -> hospDF,
            "phaDIS" -> phaDF
        )
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val gycERD = args.getOrElse("gycERD", throw new Exception("not found gycERD"))
        val hospERD = args.getOrElse("hospERD", throw new Exception("not found hospERD"))
        val prodERD = args.getOrElse("prodERD", throw new Exception("not found prodERD"))

        val gycDIS = gycERD
            .join(
                hospERD.withColumnRenamed("_id", "main-id"),
                col("HOSPITAL_ID") === col("main-id"),
                "left"
            ).drop(col("main-id"))
            .join(
                prodERD.withColumnRenamed("_id", "main-id"),
                col("PH_PRODUCT_ID") === col("main-id"),
                "left"
            ).drop(col("main-id"))

        Map(
            "gycDIS" -> gycDIS
        )
    }
}
