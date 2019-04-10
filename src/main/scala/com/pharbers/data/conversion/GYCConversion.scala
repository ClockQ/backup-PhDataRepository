package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame
import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.data.util.commonUDF.generateIdUdf

case class GYCConversion(company_id: String)(prodCvs: ProdConversion) extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val gycDF = args.getOrElse("gycDF", throw new Exception("not found gycDF"))
        val hospDF = args.getOrElse("hospDF", throw new Exception("not found hospDF"))
        val prodDF = args.getOrElse("prodDF", throw new Exception("not found prodDF"))
        val phaDF = args.getOrElse("phaDF", throw new Exception("not found phaDF"))

        val connProdHosp = {
            gycDF
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
                    ).drop(prodDF("dosage"))// 同名重复，要删掉
        }

        // 存在未成功匹配的产品, 递归执行self.toERD
        val notConnProdOfGyc = connProdHosp.filter(col("product-id").isNull)
        val notConnProdOfGycCount = notConnProdOfGyc.count()
        if (notConnProdOfGycCount != 0) {
            phDebugLog(notConnProdOfGycCount + "条产品未匹配, 重新转换")
            val notConnProdDIS = prodCvs.toDIS(prodCvs.toERD(Map("sourceDataDF" -> notConnProdOfGyc)))("prodDIS")
            return toERD(args + ("prodDF" -> notConnProdDIS.unionByName(prodDF)))
        }

        // 存在未成功匹配的医院, 递归执行self.toERD
        val notConnHospOfGyc = connProdHosp.filter(col("hosp-id").isNull)
        val notConnHospOfGycCount = notConnHospOfGyc.count()
        if (notConnHospOfGycCount != 0) {
            phDebugLog(notConnHospOfGycCount + "条医院未匹配, 重新转换")
            val notConnPhaDIS = notConnHospOfGyc.select(col("HOSP_ID"))
                    .distinct()
                    .withColumnRenamed("HOSP_ID", "GYC")
                    .withColumn("PHA_ID_NEW", generateIdUdf())
                    .cache()
            val notConnHospDIS = notConnPhaDIS.select("PHA_ID_NEW")
                    .withColumnRenamed("PHA_ID_NEW", "PHAHospId")
                    .generateId
            return toERD(args +
                    ("hospDF" -> hospDF.unionByName(notConnHospDIS.alignAt(hospDF))) +
                    ("phaDF" -> phaDF.unionByName(notConnPhaDIS.alignAt(phaDF)))
            )
        }

        val gycERD = connProdHosp
                .generateId
                .withColumn("source-id", lit(company_id))
                .str2Time
                .select("_id", "source-id", "time", "hosp-id", "product-id", "VALUE", "STANDARD_UNIT")

        Map(
            "gycERD" -> gycERD,
            "prodDIS" -> prodDF,
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
                col("hosp-id") === col("main-id"),
                "left"
            ).drop(col("main-id"))
            .join(
                prodERD.withColumnRenamed("_id", "main-id"),
                col("product-id") === col("main-id"),
                "left"
            ).drop(col("main-id"))

        Map(
            "gycDIS" -> gycDIS
        )
    }
}
