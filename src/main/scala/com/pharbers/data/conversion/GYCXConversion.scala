package com.pharbers.data.conversion

import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs}

case class GYCXConversion() extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    override def toERD(args: MapArgs): MapArgs = {
        val gycxDF = args.get.getOrElse("gycxDF", throw new Exception("not found gycxDF")).getBy[DFArgs]
        val hospDF = args.get.getOrElse("hospDF", throw new Exception("not found hospDF")).getBy[DFArgs]
        val prodDF = args.get.getOrElse("prodDF", throw new Exception("not found prodDF")).getBy[DFArgs]
        val phaDF = args.get.getOrElse("phaDF", throw new Exception("not found phaDF")).getBy[DFArgs]
        val appendProdFunc = args.get.get("appendProdFunc")

        val connProdHosp = {
            gycxDF
                    .join(
                        phaDF.drop("_id").dropDuplicates("GYC")
                        , gycxDF("HOSP_ID") === phaDF("GYC")
                        , "left"
                    )
                    .join(
                        hospDF.withColumnRenamed("_id", "HOSPITAL_ID").dropDuplicates("PHA_HOSP_ID")
                        , phaDF("PHA_ID_NEW") === hospDF("PHA_HOSP_ID")
                        , "left"
                    )
                    .join(
                        prodDF
                        , gycxDF("PRODUCT_NAME") === prodDF("ETC_PRODUCT_NAME")
                                && gycxDF("MOLE_NAME") === prodDF("ETC_MOLE_NAME")
                                && gycxDF("DOSAGE") === prodDF("ETC_DOSAGE_NAME")
                                && gycxDF("PACK_DES") === prodDF("ETC_PACKAGE_DES")
                                && gycxDF("PACK_NUMBER") === prodDF("ETC_PACKAGE_NUMBER")
                                && gycxDF("CORP_NAME") === prodDF("ETC_CORP_NAME")
                        , "left"
                    )
        }

        // 存在未成功匹配的产品, 递归执行self.toERD
        val notConnProdOfGycx = connProdHosp.filter(col("ETC_PRODUCT_ID").isNull)
        val notConnProdOfGycxCount = notConnProdOfGycx.count()
        if (notConnProdOfGycxCount != 0) {
            phDebugLog(notConnProdOfGycxCount + "条产品未匹配, 重新转换")
            appendProdFunc match {
                case Some(funcArgs) =>
                    val notConnProdDIS = funcArgs.asInstanceOf[SingleArgFuncArgs[MapArgs, MapArgs]]
                            .func(MapArgs(Map("sourceDataDF" -> DFArgs(notConnProdOfGycx))))
                            .getAs[DFArgs]("productEtcDIS")
                    return toERD(MapArgs(args.get +
                            ("prodDF" -> DFArgs(prodDF.unionByName(notConnProdDIS.alignAt(prodDF))))
                    ))
                case None => Unit
            }
        }

        // 存在未成功匹配的医院, 递归执行self.toERD
        val notConnHospOfGycx = connProdHosp.filter(col("HOSPITAL_ID").isNull)
        val notConnHospOfGycxCount = notConnHospOfGycx.count()
        if (notConnHospOfGycxCount != 0) {
            phDebugLog(notConnHospOfGycxCount + "条医院未匹配, 重新转换")
            val notConnPhaDIS = notConnHospOfGycx.select(col("HOSP_ID"))
                    .distinct()
                    .withColumnRenamed("HOSP_ID", "GYC")
                    .withColumn("PHA_ID_NEW", commonUDF.generateIdUdf())
                    .cache()

            val notConnHospDIS = notConnPhaDIS.select("PHA_ID_NEW")
                    .withColumnRenamed("PHA_ID_NEW", "PHA_HOSP_ID")
                    .generateId

            return toERD(MapArgs(args.get +
                    ("hospDF" -> DFArgs(hospDF.unionByName(notConnHospDIS.alignAt(hospDF)))) +
                    ("phaDF" -> DFArgs(phaDF.unionByName(notConnPhaDIS.alignAt(phaDF))))
            ))
        }

        val gycxERD = connProdHosp
                .generateId
                .str2Time
                .select($"_id", gycxDF("COMPANY_ID"), $"YM",
                    $"HOSPITAL_ID", $"ETC_PRODUCT_ID".as("PRODUCT_ID"),
                    $"VALUE".as("SALES"), $"STANDARD_UNIT".as("UNITS"))

        MapArgs(Map(
            "gycxERD" -> DFArgs(gycxERD),
            "prodDIS" -> DFArgs(prodDF),
            "hospDIS" -> DFArgs(hospDF),
            "phaDIS" -> DFArgs(phaDF)
        ))
    }

    override def toDIS(args: MapArgs): MapArgs = {
        val gycxERD = args.get.getOrElse("gycxERD", throw new Exception("not found gycxERD")).getBy[DFArgs]
        val hospERD = args.get.getOrElse("hospERD", throw new Exception("not found hospERD")).getBy[DFArgs]
        val prodERD = args.get.getOrElse("prodERD", throw new Exception("not found prodERD")).getBy[DFArgs]

        val gycDIS = gycxERD
                .join(
                    hospERD,
                    gycxERD("HOSPITAL_ID") === hospERD("_id"),
                    "left"
                )
                .drop(hospERD("_id"))
                .join(
                    prodERD,
                    gycxERD("PRODUCT_ID") === prodERD("_id"),
                    "left"
                )
                .drop(prodERD("_id"))


        MapArgs(Map("gycxDIS" -> DFArgs(gycDIS)))
    }

}
