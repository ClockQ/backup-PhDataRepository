package com.pharbers.data.conversion

import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, SingleArgFuncArgs, StringArgs}

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class CPAConversion()(implicit val sparkDriver: phSparkDriver) extends PhDataConversion {

    import com.pharbers.data.util._
    import sparkDriver.ss.implicits._
    import org.apache.spark.sql.functions._

    override def toERD(args: MapArgs): MapArgs = {
        val company_id = args.getAs[StringArgs]("company_id")
        val source = args.getAs[StringArgs]("source")

        val matchHospFunc = args.getAs[SingleArgFuncArgs[MapArgs, MapArgs]]("matchHospFunc")
        val matchProdFunc = args.getAs[SingleArgFuncArgs[MapArgs, MapArgs]]("matchProdFunc")

        val matchHospCpaDF = matchHospFunc(args).get.head._2.getBy[DFArgs]
        val matchDevProdDF = matchProdFunc(
            MapArgs(args.get + ("cpaDF" -> DFArgs(matchHospCpaDF)))
        ).get.head._2.getBy[DFArgs]

        val cpaERD = matchDevProdDF
                .addColumn("COMPANY_ID", company_id)
                .addColumn("SOURCE", source)
                .addColumn("PRODUCT_NAME_NOTE", "")
                .str2Time
                .select($"COMPANY_ID", $"SOURCE", $"YM", $"HOSPITAL_ID", $"PRODUCT_ID"
                    , $"VALUE".cast("double").as("SALES"), $"STANDARD_UNIT".cast("double").as("UNITS")
                    , $"PRODUCT_NAME_NOTE")
                .generateId

        MapArgs(Map("cpaERD" -> DFArgs(cpaERD)))
    }

    override def toDIS(args: MapArgs): MapArgs = {
        val cpaERD = args.get.getOrElse("cpaERD", throw new Exception("not found cpaERD")).getBy[DFArgs]
        val hospDIS = args.get.getOrElse("hospDIS", throw new Exception("not found hospDIS")).getBy[DFArgs]
        val prodDIS = args.get.getOrElse("prodDIS", throw new Exception("not found prodDIS")).getBy[DFArgs]

        val cpaDIS = cpaERD
                .join(
                    hospDIS,
                    cpaERD("HOSPITAL_ID") === hospDIS("_id"),
                    "left"
                )
                .drop(hospDIS("_id"))
                .join(
                    prodDIS.drop(prodDIS("_id")),
                    cpaERD("PRODUCT_ID") === prodDIS("DEV_PRODUCT_ID"),
                    "left"
                )

        MapArgs(Map("cpaDIS" -> DFArgs(cpaDIS)))
    }

    def matchHospFunc(args: MapArgs): MapArgs = {
        val cpaDF = args.getAs[DFArgs]("cpaDF").withColumn("HOSP_ID", $"HOSP_ID".cast("int"))
        val hospDF = args.getAs[DFArgs]("hospDF")
                .select($"_id" as "HOSPITAL_ID", $"PHA_HOSP_ID")
                .dropDuplicates("PHA_HOSP_ID")
        val phaDF = args.getAs[DFArgs]("phaDF")
                .select($"CPA", $"PHA_ID_NEW")
                .withColumn("CPA", $"CPA".cast("int"))
                .dropDuplicates("CPA")

        val resultDF = cpaDF
                .join(
                    phaDF
                    , cpaDF("HOSP_ID") === phaDF("CPA")
                    , "left"
                )
                .drop(phaDF("CPA"))


                .join(
                    hospDF
                    , phaDF("PHA_ID_NEW") === hospDF("PHA_HOSP_ID")
                    , "left"
                )
                .drop(phaDF("PHA_ID_NEW"))
                .drop(hospDF("PHA_HOSP_ID"))
                .withColumn("HOSPITAL_ID",
                    when(hospDF("HOSPITAL_ID").isNotNull, hospDF("HOSPITAL_ID"))
                            .otherwise(concat(lit("other"), cpaDF("HOSP_ID")))
                )

        MapArgs(Map("result" -> DFArgs(resultDF)))
    }

    def matchProdFunc(args: MapArgs): MapArgs = {
        val cpaDF = args.getAs[DFArgs]("cpaDF")
                .withColumn("min1", concat(
                    col("PRODUCT_NAME"),
                    col("DOSAGE"),
                    col("PACK_DES"),
                    col("PACK_NUMBER"),
                    col("CORP_NAME"))
                )
                .withColumn("min1", regexp_replace($"min1", " ", ""))
        val prodDF = args.getAs[DFArgs]("prodDF")
                .withColumn("min2", concat(
                    col("ETC_PRODUCT_NAME"),
                    col("ETC_DOSAGE_NAME"),
                    col("ETC_PACKAGE_DES"),
                    col("ETC_PACKAGE_NUMBER"),
                    col("ETC_CORP_NAME"))
                )
                .select($"DEV_PRODUCT_ID" as "PRODUCT_ID", regexp_replace($"min2", " ", "") as "min2")
        val prodMatchDF = args.getAs[DFArgs]("prodMatchDF")
                .select(
                    regexp_replace($"MIN_PRODUCT_UNIT", " ", "") as "MIN_PRODUCT_UNIT"
                    , regexp_replace($"MIN_PRODUCT_UNIT_STANDARD", " ", "") as "MIN_PRODUCT_UNIT_STANDARD"
                )

        val resultDF = cpaDF
                .join(
                    prodMatchDF
                    , cpaDF("min1") === prodMatchDF("MIN_PRODUCT_UNIT")
                    , "left"
                )
                .drop(cpaDF("min1"))
                .drop("MIN_PRODUCT_UNIT")
                .join(
                    prodDF
                    , prodMatchDF("MIN_PRODUCT_UNIT_STANDARD") === prodDF("min2")
                    , "left"
                )
                .drop("MIN_PRODUCT_UNIT_STANDARD")
                .drop(prodDF("min2"))
        val nullCount = resultDF.filter($"PRODUCT_ID".isNull).count()
        if (nullCount != 0)
            throw new Exception("cpa exist " + nullCount + " null `PRODUCT_ID`")
        MapArgs(Map("result" -> DFArgs(resultDF)))
    }
}