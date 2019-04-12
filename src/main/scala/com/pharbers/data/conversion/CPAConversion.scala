package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame
import com.pharbers.util.log.phLogTrait.phDebugLog

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class CPAConversion(company_id: String)(prodCvs: ProdConversion)
        extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.commonUDF.generateIdUdf

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val cpaDF = args.getOrElse("cpaDF", throw new Exception("not found cpaDF"))
        val hospDF = args.getOrElse("hospDF", throw new Exception("not found hospDF"))
        val prodDF = args.getOrElse("prodDF", throw new Exception("not found prodDF"))
        val phaDF = args.getOrElse("phaDF", throw new Exception("not found phaDF"))

        val connProdHosp = {
            cpaDF
                    .join(
                        phaDF.drop("_id").dropDuplicates("CPA")
                        , cpaDF("HOSP_ID") === phaDF("CPA")
                        , "left"
                    )
                    .join(
                        hospDF.withColumnRenamed("_id", "hosp-id").dropDuplicates("PHAHospId")
                        , phaDF("PHA_ID_NEW") === hospDF("PHAHospId")
                        , "left"
                    )
                    .join(
                        prodDF.withColumnRenamed("_id", "product-id")
                        , cpaDF("PRODUCT_NAME") === prodDF("product-name")
                                && cpaDF("MOLE_NAME") === prodDF("mole-name")
                                && cpaDF("DOSAGE") === prodDF("dosage")
                                && cpaDF("PACK_DES") === prodDF("package-des")
                                && cpaDF("PACK_NUMBER") === prodDF("package-number")
                                && cpaDF("CORP_NAME") === prodDF("corp-name")
                        , "left"
                    ).drop(prodDF("dosage"))// 同名重复，要删掉
        }

        // 存在未成功匹配的产品, 递归执行self.toERD
        val notConnProdOfCpa = connProdHosp.filter(col("product-id").isNull)
        val notConnProdOfCpaCount = notConnProdOfCpa.count()
        if (notConnProdOfCpaCount != 0) {
            phDebugLog(notConnProdOfCpaCount + "条产品未匹配, 重新转换")
            val notConnProdDIS = prodCvs.toDIS(prodCvs.toERD(Map("sourceDataDF" -> notConnProdOfCpa)))("prodDIS")
            return toERD(args + ("prodDF" -> notConnProdDIS.unionByName(prodDF)))
        }

        // 存在未成功匹配的医院, 递归执行self.toERD
        val notConnHospOfCpa = connProdHosp.filter(col("hosp-id").isNull)
        val notConnHospOfCpaCount = notConnHospOfCpa.count()
        if (notConnHospOfCpaCount != 0) {
            phDebugLog(notConnHospOfCpaCount + "条医院未匹配, 重新转换")
            val notConnPhaDIS = notConnHospOfCpa.select(col("HOSP_ID"))
                    .distinct()
                    .withColumnRenamed("HOSP_ID", "CPA")
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

        val cpaERD = connProdHosp
                .generateId
                .withColumn("source-id", lit(company_id))
                .str2Time
                .trim("PRODUCT_NAME_NOTE")
                .select("_id", "source-id", "TIME", "hosp-id", "product-id", "VALUE", "STANDARD_UNIT", "PRODUCT_NAME_NOTE")

        Map(
            "cpaERD" -> cpaERD,
            "prodDIS" -> prodDF,
            "hospDIS" -> hospDF,
            "phaDIS" -> phaDF
        )
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val cpaERD = args.getOrElse("cpaERD", throw new Exception("not found cpaERD"))
        val hospERD = args.getOrElse("hospERD", throw new Exception("not found hospERD"))
        val prodERD = args.getOrElse("prodERD", throw new Exception("not found prodERD"))

        val cpaDIS = cpaERD
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
            "cpaDIS" -> cpaDIS
        )
    }
}

// cpa
//+----+-----+-------+---------+------------+--------+-----------+-----+-------------+------+------------+----------+
//|YEAR|MONTH|HOSP_ID|MOLE_NAME|PRODUCT_NAME|PACK_DES|PACK_NUMBER|VALUE|STANDARD_UNIT|DOSAGE|DELIVERY_WAY| CORP_NAME|
//+----+-----+-------+---------+------------+--------+-----------+-----+-------------+------+------------+----------+
//|2018|    4| 450241|     制霉菌素|        制霉菌素| 0.5 MIU|        100|  943|         3600|    片剂|          口服|浙江震元制药有限公司|
// hosp
//+----------+--------------------+--------------------+---------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+----+
//| PHAHospId|                 _id|           addressID|character|           estimates|level|                nobs|                 noo|                 nos|            revenues|           specialty|               title|type|
//+----------+--------------------+--------------------+---------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+----+
//|PHA0025633|5c9c81a5e068ab60e...|5c9c81a4e068ab60e...|       公立|[5c9c8220e068ab60...|   二级|[5c9c81f9e068ab60...|[5c9c81f2e068ab60...|[5c9c821de068ab60...|[5c9c8202e068ab60...|[5c9c81aae068ab60...|          镇江市润州区黄山医院|   区|
// prod
//+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+------------+-------------+-----+---------+------------+----------+--------------------+--------------+---------+
//|                 _id|             corp-id|         delivery-id|           dosage-id|             mole-id|          package-id|product-name|standard-unit|value|mole-name|delivery-way|    dosage|         package-des|package-number|corp-name|
//+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+------------+-------------+-----+---------+------------+----------+--------------------+--------------+---------+
//|5c9a7a266f566f5b2...|5c9a7a4e6f566f5b2...|5c9a7a4c6f566f5b2...|5c9a7a476f566f5b2...|5c9a7a316f566f5b2...|5c9a7a3e6f566f5b2...|       拜阿司匹灵|          500|  197|     阿司匹林|          口服|       咀嚼片|                0.5g|            10|     null|
// cpaERD
//+--------------------+--------------------+-------------+--------------------+--------------------+-----------------+-----+-------------+
//|                 _id|           source-id|         time|             hosp-id|          product-id|PRODUCT_NAME_NOTE|VALUE|STANDARD_UNIT|
//+--------------------+--------------------+-------------+--------------------+--------------------+-----------------+-----+-------------+
//|5ca1822197d1244da...|5ca069e5eeefcc012...|1519833600000|5c9c81a5e068ab60e...|5c9d64646f566f33c...|             null|  733|           50|