package com.pharbers.data.job

import org.apache.spark.sql.DataFrame
import com.pharbers.pactions.actionbase._
import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.data.conversion.{HospConversion, PanelConversion}
import com.pharbers.data.util._

class Panel2ERDJob(args: Map[String, String])(implicit any: Any = null) extends sequenceJobWithMap {
    override val actions: List[pActionTrait] = Nil
    override val name: String = "Panel2ERDJob"
    import com.pharbers.data.util.spark._

    val company_id: String = args("company_id")
    val panel_file: String = args("panel_file")

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

    val source_file: String = args("source_file")

    val panelCvs = PanelConversion(company_id)
    val hospCvs = HospConversion()

    override def perform(pr: pActionArgs): pActionArgs = {

        phDebugLog("开始转换:" + panel_file)
        val panelDF = Parquet2DF(panel_file)
        val panelDFCount: Long = panelDF.count()

        val hospDIS: DataFrame = ???
//        hospCvs.toDIS {
//            val args = Map.newBuilder[String, DataFrame]
//            args += "hospBaseERD" -> Parquet2DF(hosp_base_file)
////            if (hosp_bed_file.nonEmpty) args += "hospBedERD" -> Parquet2DF(hosp_bed_file)
////            if (hosp_estimate_file.nonEmpty) args += "hospEstimateERD" -> Parquet2DF(hosp_estimate_file)
////            if (hosp_outpatient_file.nonEmpty) args += "hospOutpatientERD" -> Parquet2DF(hosp_outpatient_file)
////            if (hosp_revenue_file.nonEmpty) args += "hospRevenueERD" -> Parquet2DF(hosp_revenue_file)
////            if (hosp_specialty_file.nonEmpty) args += "hospSpecialtyERD" -> Parquet2DF(hosp_specialty_file)
////            if (hosp_staffnum_file.nonEmpty) args += "hospStaffNumERD" -> Parquet2DF(hosp_staffnum_file)
////            if (hosp_unit_file.nonEmpty) args += "hospUnitERD" -> Parquet2DF(hosp_unit_file)
//            args.result()
//        }("hospDIS")
        val hospDISCount: Long = hospDIS.count()

        val sourceDF = Parquet2DF(source_file)
        val sourceDFCount = sourceDF.count()
        val panelResult: Map[String, DataFrame] = panelCvs.toERD(
            Map(
                "panelDF" -> panelDF,
                "hospDF" -> hospDIS,
                "sourceDF" -> Parquet2DF(source_file)
            )
        )

        val panelERD = panelResult("panelERD")
        val panelERDCount: Long = panelERD.count()
        val panelHosp = panelResult("hospDIS")
        val panelHospCount: Long = panelHosp.count()
        val panelSource = panelResult("sourceDIS")
        val panelSourceCount: Long = panelSource.count()

        if (panelDFCount != panelERDCount) throw new Exception(s"转换后条目不对$panelDFCount -> $panelERDCount")
        if (hospDISCount != panelHospCount) throw new Exception(s"转换后与医院大全对应不对$hospDISCount -> $panelHospCount")
        if (sourceDFCount != panelSourceCount) throw new Exception(s"转换后与source对应不对$sourceDFCount -> $panelSourceCount")



        MapArgs(Map(
            "result" -> StringArgs("Conversion success")
        ))
    }
}
