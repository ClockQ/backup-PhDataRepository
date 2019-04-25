package com.pharbers.data.conversion

import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs}

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class CPAConversion() extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    override def toERD(args: MapArgs): MapArgs = {
        val cpaDF = args.get.getOrElse("cpaDF", throw new Exception("not found cpaDF")).getBy[DFArgs]
        val hospDF = args.get.getOrElse("hospDF", throw new Exception("not found hospDF")).getBy[DFArgs]
        val prodDF = args.get.getOrElse("prodDF", throw new Exception("not found prodDF")).getBy[DFArgs]
        val phaDF = args.get.getOrElse("phaDF", throw new Exception("not found phaDF")).getBy[DFArgs]
        val appendProdFunc = args.get.getOrElse("appendProdFunc", throw new Exception("not found appendProdFunc")).getBy[SingleArgFuncArgs[MapArgs, MapArgs]]

        val connProdHosp = {
            cpaDF
                    .join(
                        phaDF.drop("_id").dropDuplicates("CPA")
                        , cpaDF("HOSP_ID") === phaDF("CPA")
                        , "left"
                    )
                    .join(
                        hospDF.withColumnRenamed("_id", "HOSPITAL_ID").dropDuplicates("PHAHospId")
                        , phaDF("PHA_ID_NEW") === hospDF("PHAHospId")
                        , "left"
                    )
                    .join(
                        prodDF
                        , cpaDF("PRODUCT_NAME") === prodDF("ETC_PRODUCT_NAME")
                                && cpaDF("MOLE_NAME") === prodDF("ETC_MOLE_NAME")
                                && cpaDF("DOSAGE") === prodDF("ETC_DOSAGE_NAME")
                                && cpaDF("PACK_DES") === prodDF("ETC_PACKAGE_DES")
                                && cpaDF("PACK_NUMBER") === prodDF("ETC_PACKAGE_NUMBER")
                                && cpaDF("CORP_NAME") === prodDF("ETC_CORP_NAME")
                        , "left"
                    )
        }

        // 存在未成功匹配的产品, 递归执行self.toERD
        val notConnProdOfCpa = connProdHosp.filter(col("ETC_PRODUCT_ID").isNull)
        val notConnProdOfCpaCount = notConnProdOfCpa.count()
        if (notConnProdOfCpaCount != 0) {
            phDebugLog(notConnProdOfCpaCount + "条产品未匹配, 重新转换")
            val notConnProdDIS = appendProdFunc(MapArgs(Map("sourceDataDF" -> DFArgs(notConnProdOfCpa))))
                    .getAs[DFArgs]("productEtcDIS")
            return toERD(MapArgs(args.get +
                    ("prodDF" -> DFArgs(prodDF.unionByName(notConnProdDIS.alignAt(prodDF))))
            ))
        }

        // 存在未成功匹配的医院, 递归执行self.toERD
        val notConnHospOfCpa = connProdHosp.filter(col("HOSPITAL_ID").isNull)
        val notConnHospOfCpaCount = notConnHospOfCpa.count()
        if (notConnHospOfCpaCount != 0) {
            phDebugLog(notConnHospOfCpaCount + "条医院未匹配, 重新转换")
            val notConnPhaDIS = notConnHospOfCpa.select(col("HOSP_ID"))
                    .distinct()
                    .withColumnRenamed("HOSP_ID", "CPA")
                    .withColumn("PHA_ID_NEW", commonUDF.generateIdUdf())
                    .cache()

            val notConnHospDIS = notConnPhaDIS.select("PHA_ID_NEW")
                    .withColumnRenamed("PHA_ID_NEW", "PHAHospId")
                    .generateId

            return toERD(MapArgs(args.get +
                    ("hospDF" -> DFArgs(hospDF.unionByName(notConnHospDIS.alignAt(hospDF)))) +
                    ("phaDF" -> DFArgs(phaDF.unionByName(notConnPhaDIS.alignAt(phaDF))))
            ))
        }

        val cpaERD = connProdHosp
                .generateId
                .str2Time
                .trim("PRODUCT_NAME_NOTE")
                .select($"_id", cpaDF("COMPANY_ID"), $"YM",
                    $"HOSPITAL_ID".as("HOSP_ID"), $"ETC_PRODUCT_ID".as("PRODUCT_ID"),
                    $"VALUE".as("SALES"), $"STANDARD_UNIT".as("UNITS"), $"PRODUCT_NAME_NOTE")

        MapArgs(Map(
            "cpaERD" -> DFArgs(cpaERD),
            "prodDIS" -> DFArgs(prodDF),
            "hospDIS" -> DFArgs(hospDF),
            "phaDIS" -> DFArgs(phaDF)
        ))
    }

    override def toDIS(args: MapArgs): MapArgs = {
        val cpaERD = args.get.getOrElse("cpaERD", throw new Exception("not found cpaERD")).getBy[DFArgs]
        val hospERD = args.get.getOrElse("hospERD", throw new Exception("not found hospERD")).getBy[DFArgs]
        val prodERD = args.get.getOrElse("prodERD", throw new Exception("not found prodERD")).getBy[DFArgs]

        val cpaDIS = cpaERD
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

        MapArgs(Map("cpaDIS" -> DFArgs(cpaDIS)))
    }
}