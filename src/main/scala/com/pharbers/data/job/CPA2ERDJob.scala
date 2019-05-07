package com.pharbers.data.job

import org.apache.spark.sql.DataFrame
import com.pharbers.pactions.actionbase._
import org.apache.spark.sql.functions.{col, when}
import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.data.conversion.{CPAConversion, HospConversion, ProductEtcConversion2}

/**
  * @description:
  * @author: clock
  * @date: 2019-04-08 10:46
  */
case class CPA2ERDJob(args: Map[String, String])(implicit any: Any = null) extends sequenceJobWithMap {
    override val name: String = "CPA2ERDJob"
    override val actions: List[pActionTrait] = Nil

    import com.pharbers.data.util._

    val company_id: String = args("company_id")

    val cpa_file: String = args("cpa_file")
    val pha_file: String = args("pha_file")

    val hosp_base_file: String = args("hosp_base_file")
    val hosp_bed_file: String = args.getOrElse("hosp_bed_file", "")
    val hosp_estimate_file: String = args.getOrElse("hosp_estimate_file", "")
    val hosp_outpatient_file: String = args.getOrElse("hosp_outpatient_file", "")
    val hosp_revenue_file: String = args.getOrElse("hosp_revenue_file", "")
    val hosp_specialty_file: String = args.getOrElse("hosp_specialty_file", "")
    val hosp_staffnum_file: String = args.getOrElse("hosp_staffnum_file", "")
    val hosp_unit_file: String = args.getOrElse("hosp_unit_file", "")
    val hosp_address_file: String = args.getOrElse("hosp_address_file", "")
    val hosp_prefecture_file: String = args.getOrElse("hosp_prefecture_file", "")
    val hosp_city_file: String = args.getOrElse("hosp_city_file", "")
    val hosp_province_file: String = args.getOrElse("hosp_province_file", "")

    val prod_etc_file: String = args("prod_etc_file")
    val prod_atc_file: String = args.getOrElse("prod_atc_file", "")
    val prod_market_file: String = args.getOrElse("prod_market_file", "")
    val prod_dev_file: String = args.getOrElse("prod_dev_file", "")
    val prod_match_file: String = args.getOrElse("prod_match_file", "")

    val hosp_base_file_temp: String = args.getOrElse("hosp_base_file_temp", "")
    val prod_etc_file_temp: String = args.getOrElse("prod_etc_file_temp", "")
    val pha_file_temp: String = args.getOrElse("pha_file_temp", "")

    val hospCvs: HospConversion = HospConversion()
    val prodCvs: ProductEtcConversion2 = ProductEtcConversion2()
    val cpaCvs: CPAConversion = CPAConversion()

    override def perform(pr: pActionArgs = MapArgs(Map())): pActionArgs = {
        phDebugLog("开始转换:" + cpa_file)
        val cpaDF = CSV2DF(cpa_file)
        val cpaDFCount: Long = cpaDF.count()
        val phaDF = Parquet2DF(pha_file)
        val phaDFCount: Long = phaDF.count()

        val hospDIS: DataFrame = hospCvs.toDIS(MapArgs {
            val args = Map.newBuilder[String, DFArgs]
            args += "hospBaseERD" -> DFArgs(Parquet2DF(hosp_base_file))
            if (hosp_bed_file.nonEmpty) args += "hospBedERD" -> DFArgs(Parquet2DF(hosp_bed_file))
            if (hosp_estimate_file.nonEmpty) args += "hospEstimateERD" -> DFArgs(Parquet2DF(hosp_estimate_file))
            if (hosp_outpatient_file.nonEmpty) args += "hospOutpatientERD" -> DFArgs(Parquet2DF(hosp_outpatient_file))
            if (hosp_revenue_file.nonEmpty) args += "hospRevenueERD" -> DFArgs(Parquet2DF(hosp_revenue_file))
            if (hosp_specialty_file.nonEmpty) args += "hospSpecialtyERD" -> DFArgs(Parquet2DF(hosp_specialty_file))
            if (hosp_staffnum_file.nonEmpty) args += "hospStaffNumERD" -> DFArgs(Parquet2DF(hosp_staffnum_file))
            if (hosp_unit_file.nonEmpty) args += "hospUnitERD" -> DFArgs(Parquet2DF(hosp_unit_file))
            if (hosp_address_file.nonEmpty) args += "hospAddressERD" -> DFArgs(Parquet2DF(hosp_address_file))
            if (hosp_prefecture_file.nonEmpty) args += "hospPrefectureERD" -> DFArgs(Parquet2DF(hosp_prefecture_file))
            if (hosp_city_file.nonEmpty) args += "hospCityERD" -> DFArgs(Parquet2DF(hosp_city_file))
            if (hosp_province_file.nonEmpty) args += "hospProvinceERD" -> DFArgs(Parquet2DF(hosp_province_file))
            args.result()
        }).getAs[DFArgs]("hospDIS")
        val hospDISCount: Long = hospDIS.count()

        val productEtcDIS: DataFrame = prodCvs.toDIS(MapArgs{
            val args = Map.newBuilder[String, DFArgs]
            args += "productEtcERD" -> DFArgs(Parquet2DF(prod_etc_file))
            if (prod_atc_file.nonEmpty) args += "atcERD" -> DFArgs(Parquet2DF(prod_atc_file))
            if (prod_market_file.nonEmpty) args += "marketERD" -> DFArgs(Parquet2DF(prod_market_file))
            if (prod_dev_file.nonEmpty) args += "productDevERD" -> DFArgs(Parquet2DF(prod_dev_file))
            if (prod_match_file.nonEmpty) args += "productMatchDF" -> DFArgs(
                Parquet2DF(prod_match_file)
                        .addColumn("PACK_NUMBER")
                        .addColumn("PACK_COUNT")
                        .withColumn("PACK_NUMBER",
                            when(col("PACK_NUMBER").isNotNull,
                                col("PACK_NUMBER")
                            ).otherwise(col("PACK_COUNT")))
            )
            args.result()
        }).getAs[DFArgs]("productEtcDIS")
        val productEtcDISCount = productEtcDIS.count()

        val result = cpaCvs.toERD(MapArgs(Map(
            "cpaDF" -> DFArgs(cpaDF.addColumn("COMPANY_ID", company_id).addColumn("SOURCE", "CPA"))
            , "hospDF" -> DFArgs(hospDIS)
            , "prodDF" -> DFArgs(productEtcDIS)
            , "phaDF" -> DFArgs(phaDF)
            , "appendProdFunc" -> SingleArgFuncArgs { args: MapArgs =>
                prodCvs.toDIS(prodCvs.toERD(args))
            }
        )))

        val cpaERD = result.getAs[DFArgs]("cpaERD")
        val cpaERDCount: Long = cpaERD.count()
        val cpaProd = result.getAs[DFArgs]("prodDIS")
        val cpaProdCount: Long = cpaProd.count()
        val cpaHosp = result.getAs[DFArgs]("hospDIS")
        val cpaHospCount: Long = cpaHosp.count()
        val cpaPha = result.getAs[DFArgs]("phaDIS")
        val cpaPhaCount: Long = cpaPha.count()

        if (cpaDFCount != cpaERDCount) throw new Exception(s"转换后条目不对$cpaDFCount -> $cpaERDCount")
        if (productEtcDISCount != cpaProdCount && prod_etc_file_temp.nonEmpty) cpaProd.save2Parquet(prod_etc_file_temp)
        if (hospDISCount != cpaHospCount && hosp_base_file_temp.nonEmpty) cpaHosp.save2Parquet(hosp_base_file_temp)
        if (phaDFCount != cpaPhaCount && pha_file_temp.nonEmpty) cpaPha.save2Parquet(pha_file_temp)

        MapArgs(Map(
            "result" -> StringArgs("Conversion success")
        ))
    }
}