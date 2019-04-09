package com.pharbers.data.job

import org.apache.spark.sql.DataFrame
import com.pharbers.pactions.actionbase._
import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.data.conversion.{CPAConversion, HospConversion, ProdConversion}

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

    val prod_base_file: String = args("prod_base_file")
    val prod_delivery_file: String = args("prod_delivery_file")
    val prod_dosage_file: String = args("prod_dosage_file")
    val prod_mole_file: String = args("prod_mole_file")
    val prod_package_file: String = args("prod_package_file")
    val prod_corp_file: String = args("prod_corp_file")

    val save_prod_file: String = args.getOrElse("save_prod_file", "")
    val save_hosp_file: String = args.getOrElse("save_hosp_file", "")
    val save_pha_file: String = args.getOrElse("save_pha_file", "")

    val hospCvs: HospConversion = HospConversion()
    val prodCvs: ProdConversion = ProdConversion()
    val cpaCvs: CPAConversion = CPAConversion(company_id)(prodCvs)

    override def perform(pr: pActionArgs = MapArgs(Map())): pActionArgs = {
        phDebugLog("开始转换:" + cpa_file)
        val cpaDF = CSV2DF(cpa_file)
        val cpaDFCount: Long = cpaDF.count()
        val phaDF = Parquet2DF(pha_file)
        val phaDFCount: Long = phaDF.count()

        val hospDIS: DataFrame = hospCvs.toDIS {
            val args = Map.newBuilder[String, DataFrame]
            args += "hospBaseERD" -> Parquet2DF(hosp_base_file)
            if (hosp_bed_file.nonEmpty) args += "hospBedERD" -> Parquet2DF(hosp_bed_file)
            if (hosp_estimate_file.nonEmpty) args += "hospEstimateERD" -> Parquet2DF(hosp_estimate_file)
            if (hosp_outpatient_file.nonEmpty) args += "hospOutpatientERD" -> Parquet2DF(hosp_outpatient_file)
            if (hosp_revenue_file.nonEmpty) args += "hospRevenueERD" -> Parquet2DF(hosp_revenue_file)
            if (hosp_specialty_file.nonEmpty) args += "hospSpecialtyERD" -> Parquet2DF(hosp_specialty_file)
            if (hosp_staffnum_file.nonEmpty) args += "hospStaffNumERD" -> Parquet2DF(hosp_staffnum_file)
            if (hosp_unit_file.nonEmpty) args += "hospUnitERD" -> Parquet2DF(hosp_unit_file)
            args.result()
        }("hospDIS")
        val hospDISCount: Long = hospDIS.count()
        val prodDIS: DataFrame = prodCvs.toDIS {
            val args = Map.newBuilder[String, DataFrame]
            args += "prodBaseERD" -> Parquet2DF(prod_base_file)
            if (prod_delivery_file.nonEmpty) args += "prodDeliveryERD" -> Parquet2DF(prod_delivery_file)
            if (prod_dosage_file.nonEmpty) args += "prodDosageERD" -> Parquet2DF(prod_dosage_file)
            if (prod_mole_file.nonEmpty) args += "prodMoleERD" -> Parquet2DF(prod_mole_file)
            if (prod_package_file.nonEmpty) args += "prodPackageERD" -> Parquet2DF(prod_package_file)
            if (prod_corp_file.nonEmpty) args += "prodCorpERD" -> Parquet2DF(prod_corp_file)
            args.result()
        }("prodDIS")
        val prodDISCount: Long = prodDIS.count()

        val cpaResult: Map[String, DataFrame] = cpaCvs.toERD(
            Map(
                "cpaDF" -> cpaDF,
                "hospDF" -> hospDIS,
                "prodDF" -> prodDIS,
                "phaDF" -> phaDF
            )
        )
        val cpaERD = cpaResult("cpaERD")
        val cpaERDCount: Long = cpaERD.count()
        val cpaProd = cpaResult("prodDIS")
        val cpaProdCount: Long = cpaProd.count()
        val cpaHosp = cpaResult("hospDIS")
        val cpaHospCount: Long = cpaHosp.count()
        val cpaPha = cpaResult("phaDIS")
        val cpaPhaCount: Long = cpaPha.count()

        if (cpaDFCount != cpaERDCount) throw new Exception(s"转换后条目不对$cpaDFCount -> $cpaERDCount")
        if (prodDISCount != cpaProdCount && save_prod_file.nonEmpty) cpaProd.save2Parquet(save_prod_file)
        if (hospDISCount != cpaHospCount && save_hosp_file.nonEmpty) cpaHosp.save2Parquet(save_hosp_file)
        if (phaDFCount != cpaPhaCount && save_pha_file.nonEmpty) cpaPha.save2Parquet(save_pha_file)

        MapArgs(Map(
            "result" -> StringArgs("Conversion success")
        ))
    }
}
