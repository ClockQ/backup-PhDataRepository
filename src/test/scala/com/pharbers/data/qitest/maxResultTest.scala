package com.pharbers.data.qitest

import com.pharbers.data.util.ParquetLocation.{HOSP_BASE_LOCATION, HOSP_DIS_LOCATION}

/**
  * @description:
  * @author: clock
  * @date: 2019-04-29 13:50
  */
object maxResultTest extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.util.spark._

    import sparkDriver.ss.implicits._
    import org.apache.spark.sql.functions._


    val nhwaHosp = CSV2DF("hdfs:///data/nhwa/pha_config_repository1804/Nhwa_2018_If_panel_all_麻醉市场_20180629.csv")
    lazy val hospBaseERD = Parquet2DF(HOSP_BASE_LOCATION)
    hospBaseERD.filter($"title" === "梅河口市医院").show(false)
    nhwaHosp.show(false)
    println(nhwaHosp.count(), hospBaseERD.count())
    nhwaHosp.filter($"PHA_HOSP_ID" === "PHA0010149").show(false)
    val panelTrue = Parquet2DF("/test/qi/qi/panel_true")
    val panel5 = Parquet2DF("/test/qi/qi/panel5")
    println(panelTrue.count(), panel5.count())
    lazy val hospDIS = Parquet2DF(HOSP_DIS_LOCATION)
    val panel5DIS = panel5.join(hospDIS, panel5("HOSPITAL_ID") === hospDIS("_id"), "left")
    println(panel5DIS.count())
    val a = panelTrue.select("HOSP_ID").distinct()
    val b = panel5DIS.select("PHA_HOSP_ID", "HOSPITAL_ID")
    val c = a.join(b, a("HOSP_ID") === b("PHA_HOSP_ID"), "left").filter($"PHA_HOSP_ID".isNull)
    println(c.count())
    c.show(false)
    hospDIS.filter($"PHA_HOSP_ID" === "PHA0010149").show(false)
    hospBaseERD.filter($"PHAHospId" === "PHA0010149").show(false)
    val d = hospBaseERD.select("PHAHospId")
    println(hospBaseERD.count(), d.count())


//    val maxTrue = Parquet2DF("/test/qi/qi/max_true")
//    val max5 = Parquet2DF("/test/qi/qi/max5")
//    maxTrue.show(false)
//    max5.show(false)
//    println(maxTrue.count(), max5.count())
//
//    println(maxTrue.agg(sum("f_units")).first.get(0))
//    println(maxTrue.agg(sum("f_sales")).first.get(0))
//
//    println(max5.agg(sum("f_units")).first.get(0))
//    println(max5.agg(sum("f_sales")).first.get(0))

//    val maxDF = Parquet2DF("/test/qi/qi/max5")
//    println(maxDF.select("HOSPITAL_ID").distinct().count())
//    println(maxDF.select("PRODUCT_ID").distinct().count())
//    println(maxDF.agg(sum("f_sales")).first.get(0))
//    println(maxDF.agg(sum("f_units")).first.get(0))
//    CSV2DF("/data/nhwa/pha_config_repository1809/Nhwa_201809_Offline_MaxResult_20181126.csv").show(false)

}
