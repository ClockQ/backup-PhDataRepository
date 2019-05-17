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
    val notPublishedHospFile = "hdfs:///data/nhwa/pha_config_repository1809/Nhwa_2018_NotPublishedHosp_20180629.csv"
    val sampleFile = "hdfs:///data/nhwa/pha_config_repository1804/Nhwa_2018_If_panel_all_麻醉市场_20180629.csv"
    val universeFile = "hdfs:///data/nhwa/pha_config_repository1809/Nhwa_universe_麻醉市场_20180705.csv"

    lazy val phaDF = Parquet2DF(HOSP_PHA_LOCATION).distinctByKey("CPA")()
    lazy val hospDIS = Parquet2DF(HOSP_DIS_LOCATION).select($"HOSPITAL_ID", $"PHA_HOSP_ID").distinctByKey("PHA_HOSP_ID")()

    lazy val missHospERD = {
        val missHospDF = CSV2DF(missHospFile)
                .withColumn("DATE", explode(split($"MONTH", "、")))
                .withColumn("DATE", when($"DATE" < 10, concat(lit("0"), $"DATE")).otherwise($"DATE"))
                .withColumn("DATE", concat(lit(year), $"DATE"))
                .withColumn("HOSP_ID", $"HOSP_ID".cast("int"))
                .drop("MONTH")

        val missHospERD = missHospDF
                .join(phaDF, missHospDF("HOSP_ID") === phaDF("CPA"), "left")
                .join(hospDIS, phaDF("PHA_ID_NEW") === hospDIS("PHA_HOSP_ID"), "left")
                .withColumn("HOSPITAL_ID",
                    when(hospDIS("HOSPITAL_ID").isNotNull, hospDIS("HOSPITAL_ID"))
                            .otherwise(concat(lit("other"), missHospDF("HOSP_ID")))
                )
                .select($"DATE", $"HOSPITAL_ID")
                .distinct()
                .generateId

        missHospERD.show(false)
        lazy val minus = missHospDF.count() - missHospERD.count()
        println(minus, minus, minus, minus)
        assert(minus == 0, "转换后的ERD比源数据减少`" + minus + "`条记录")
        missHospERD
    }

    lazy val notPublishedERD = {
        val notPublishedDF = CSV2DF(notPublishedHospFile).withColumn("HOSP_ID", $"HOSP_ID".cast("int"))

        val notPublishedERD = notPublishedDF
                .join(phaDF, $"HOSP_ID" === phaDF("CPA"), "left")
                .join(hospDIS, phaDF("PHA_ID_NEW") === hospDIS("PHA_HOSP_ID"), "left")
                .withColumn("HOSPITAL_ID",
                    when(hospDIS("HOSPITAL_ID").isNotNull, hospDIS("HOSPITAL_ID"))
                            .otherwise(concat(lit("other"), $"HOSP_ID"))
                )
                .select("HOSPITAL_ID")
                .distinct()
                .generateId

        notPublishedERD.show(false)
        lazy val minus = notPublishedDF.count() - notPublishedERD.count()
        println(minus, minus, minus, minus)
        assert(minus == 0, "转换后的ERD比源数据减少`" + minus + "`条记录")
        notPublishedERD
    }

    lazy val sampleERD = { // 1038
        val sampleERD = CSV2DF(sampleFile)
                .withColumn("HOSP_ID", $"HOSP_ID".cast("int"))
                .filter($"HOSP_ID".isNotNull && $"HOSP_ID" =!= 0)//1251
                .join(phaDF, $"HOSP_ID" === phaDF("CPA"), "left")//1251
                .join(hospDIS, phaDF("PHA_ID_NEW") === hospDIS("PHA_HOSP_ID"), "left")//1251
                .withColumn("HOSPITAL_ID", when(hospDIS("HOSPITAL_ID").isNotNull, hospDIS("HOSPITAL_ID")).otherwise(concat(lit("other"), $"HOSP_ID")))//1251
                .drop(hospDIS("PHA_HOSP_ID"))
                .drop(hospDIS("HOSPITAL_ID"))
                .select($"HOSPITAL_ID", $"PHA_HOSP_NAME", $"MARKET", $"IF_PANEL_ALL" as "SAMPLE")
                .distinctByKey("HOSPITAL_ID", "SAMPLE")()//1248
                .generateId

        sampleERD.show(false)
        sampleERD
    }

    lazy val universeDF = {
        val phaDF = Parquet2DF(HOSP_PHA_LOCATION)
        val universeDF = CSV2DF(universeFile)

        val resultDF = universeDF
                .withColumnRenamed("PHA_HOSP_ID", "PHA_ID_OLD")
                .join(phaDF, $"PHA_ID_OLD" === phaDF("PHA_ID"), "left")
                .join(
                    hospDIS
                    , phaDF("PHA_ID_NEW") === hospDIS("PHA_HOSP_ID")
                    , "left"
                )
                .withColumn("HOSPITAL_ID",
                    when(hospDIS("HOSPITAL_ID").isNotNull, hospDIS("HOSPITAL_ID"))
                            .otherwise(concat(lit("other"), $"PHA_ID"))
                )
                .distinctByKey("HOSPITAL_ID")("IF_PANEL_TO_USE", max)
                .generateId

        resultDF.show(false)
        resultDF
    }

    if (args.nonEmpty && args(0) == "TRUE")
        missHospERD.save2Parquet(MISS_HOSP_LOCATION + "/" + company_id)

    if (args.nonEmpty && args(0) == "TRUE")
        notPublishedERD.save2Parquet(NOT_PUBLISHED_HOSP_LOCATION + "/" + company_id)

    if (args.nonEmpty && args(0) == "TRUE")
        sampleERD.save2Parquet(SAMPLE_HOSP_LOCATION + "/" + company_id + "/" + "mz")

    if (args.nonEmpty && args(0) == "TRUE")
        universeDF.save2Parquet(UNIVERSE_HOSP_LOCATION + "/" + company_id + "/" + "mz")
}
