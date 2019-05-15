package com.pharbers.data.run

/**
  * @description:
  * @author: clock
  * @date: 2019-04-16 19:24
  */
object TransformMissHosp extends App {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._

    import com.pharbers.data.util.spark._
    import sparkDriver.ss.implicits._

    val year = "2018"
    val company_id = NHWA_COMPANY_ID

    val missHospFile = "hdfs:///data/nhwa/pha_config_repository1809/Missing_Hospital.csv"
    val notPublishedHospFile = "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_2018_NotPublishedHosp_20180629.csv"
    val sampleFile = "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_2018_If_panel_all_麻醉市场_20180629.csv"
    val universeFile = "hdfs:///data/nhwa/pha_config_repository1809/Nhwa_universe_麻醉市场_20180705.csv"

    val phaDF = Parquet2DF(HOSP_PHA_LOCATION)
    val hospDIS = Parquet2DF(HOSP_DIS_LOCATION)

    val missHospDF = CSV2DF(missHospFile)
            .withColumn("DATE", explode(split($"MONTH", "、")))
            .withColumn("DATE", when($"DATE" < 10, concat(lit("0"), $"DATE")).otherwise($"DATE"))
            .withColumn("DATE", concat(lit(year), $"DATE"))
            .drop("MONTH")

    val missHospERD = missHospDF.join(phaDF, missHospDF("HOSP_ID") === phaDF("CPA"), "left")
            .join(hospDIS, phaDF("PHA_ID_NEW") === hospDIS("PHA_HOSP_ID"), "left")
            .select($"DATE", hospDIS("_id") as "HOSPITAL_ID")
            .filter($"HOSPITAL_ID".isNotNull)
            .distinct()
            .generateId

    missHospERD.show(false)

    val notPublishedERD = CSV2DF(notPublishedHospFile)
            .join(phaDF, $"HOSP_ID" === phaDF("CPA"), "left")
            .join(hospDIS, phaDF("PHA_ID_NEW") === hospDIS("PHA_HOSP_ID"), "left")
            .select(hospDIS("_id") as "HOSPITAL_ID")
            .filter($"HOSPITAL_ID".isNotNull)
            .distinct()
            .generateId

    notPublishedERD.show(false)

    val sampleERD = CSV2DF(sampleFile)
            .join(phaDF, $"HOSP_ID" === phaDF("CPA"), "left")
            .join(hospDIS, phaDF("PHA_ID_NEW") === hospDIS("PHA_HOSP_ID"), "left")
            .select($"MARKET", $"IF_PANEL_ALL" as "SAMPLE", hospDIS("_id") as "HOSPITAL_ID")
            .distinct()
            .generateId

    sampleERD.show(false)

    val universeDF = CSV2DF(universeFile).withColumnRenamed("PHA_HOSP_ID", "PHA_ID")
            .join(
                hospDIS.select($"_id", $"PHA_HOSP_ID")
                , $"PHA_ID" === hospDIS("PHA_HOSP_ID")
                , "left"
            )
            .drop($"PHA_ID")
            .drop(hospDIS("PHA_HOSP_ID"))
            .withColumnRenamed("_id", "HOSPITAL_ID")
            .distinct()
            .generateId

    universeDF.show(false)

    if (args.nonEmpty && args(0) == "TRUE")
        missHospERD.save2Parquet(MISS_HOSP_LOCATION + "/" + company_id)

    if (args.nonEmpty && args(0) == "TRUE")
        notPublishedERD.save2Parquet(NOT_PUBLISHED_HOSP_LOCATION + "/" + company_id)

    if (args.nonEmpty && args(0) == "TRUE")
        sampleERD.save2Parquet(SAMPLE_HOSP_LOCATION + "/" + company_id + "/" + "mz")

    if (args.nonEmpty && args(0) == "TRUE")
        universeDF.save2Parquet(UNIVERSE_HOSP_LOCATION + "/" + company_id + "/" + "mz")
}
