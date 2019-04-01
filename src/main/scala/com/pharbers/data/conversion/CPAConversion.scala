package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class CPAConversion(company: String, source_id: String)
        extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val cpaDF = args.getOrElse("cpaDF", throw new Exception("not found cpaDF"))
        val hospDF = args.getOrElse("hospDF", throw new Exception("not found hospDF"))
        val prodDF = args.getOrElse("prodDF", throw new Exception("not found prodDF"))
        val phaDF = args.getOrElse("phaDF", throw new Exception("not found phaDF"))

        val revenueDF = Seq.empty[String].toDF("_id")

        val cpaConnProdHosp = cpaDF
                .withColumn("source-id", functions.lit(source_id))
                .str2Time
                .join(
                    phaDF.drop("_id")
                    , cpaDF("HOSP_ID") === phaDF("CPA")
                    , "left"
                )
                .join(
                    hospDF.withColumnRenamed("_id", "hosp-id")
                    , phaDF("PHA_ID_NEW") === hospDF("PHAHospId")
                    , "left"
                )
                .join(
                    prodDF.withColumnRenamed("_id", "product-id")
                    , cpaDF("PRODUCT_NAME") === prodDF("product-name")
                            && cpaDF("DOSAGE") === prodDF("dosage")
                            && cpaDF("PACK_DES") === prodDF("package-des")
                            && cpaDF("PACK_NUMBER") === prodDF("package-number")
                            && cpaDF("CORP_NAME") === prodDF("corp-name")
                    , "left"
                )
                //.select("source-id", "time", "hosp-id", "product-id", "revenues-ids", "product_name_note")
                .generateId

        Map(
            "cpaDF" -> cpaConnProdHosp
            , "revenueDF" -> revenueDF
        )
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = ???
}

// cpa
//        +----+-----+-------+---------+------------+--------+-----------+-----+-------------+------+------------+----------+
//        |YEAR|MONTH|HOSP_ID|MOLE_NAME|PRODUCT_NAME|PACK_DES|PACK_NUMBER|VALUE|STANDARD_UNIT|DOSAGE|DELIVERY_WAY| CORP_NAME|
//        +----+-----+-------+---------+------------+--------+-----------+-----+-------------+------+------------+----------+
//        |2018|    4| 450241|     制霉菌素|        制霉菌素| 0.5 MIU|        100|  943|         3600|    片剂|          口服|浙江震元制药有限公司|
// hosp
//        +----------+--------------------+--------------------+---------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+----+
//        | PHAHospId|                 _id|           addressID|character|           estimates|level|                nobs|                 noo|                 nos|            revenues|           specialty|               title|type|
//        +----------+--------------------+--------------------+---------+--------------------+-----+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+----+
//        |PHA0025633|5c9c81a5e068ab60e...|5c9c81a4e068ab60e...|       公立|[5c9c8220e068ab60...|   二级|[5c9c81f9e068ab60...|[5c9c81f2e068ab60...|[5c9c821de068ab60...|[5c9c8202e068ab60...|[5c9c81aae068ab60...|          镇江市润州区黄山医院|   区|
// prod
//        +--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+------------+-------------+-----+---------+------------+----------+--------------------+--------------+---------+
//        |                 _id|             corp-id|         delivery-id|           dosage-id|             mole-id|          package-id|product-name|standard-unit|value|mole-name|delivery-way|    dosage|         package-des|package-number|corp-name|
//                +--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+------------+-------------+-----+---------+------------+----------+--------------------+--------------+---------+
//        |5c9a7a266f566f5b2...|5c9a7a4e6f566f5b2...|5c9a7a4c6f566f5b2...|5c9a7a476f566f5b2...|5c9a7a316f566f5b2...|5c9a7a3e6f566f5b2...|       拜阿司匹灵|          500|  197|     阿司匹林|          口服|       咀嚼片|                0.5g|            10|     null|