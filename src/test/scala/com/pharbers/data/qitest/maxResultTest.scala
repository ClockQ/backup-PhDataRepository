package com.pharbers.data.qitest

/**
  * @description:
  * @author: clock
  * @date: 2019-04-29 13:50
  */
object maxResultTest extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.util.spark._

    import org.apache.spark.sql.functions._
    import sparkDriver.ss.implicits._

    val cpaDF = CSV2DF("hdfs:///data/nhwa/pha_config_repository1809/Nhwa_201809_CPA_20181126.csv")
    val cpaERD = Parquet2DF("/workData/Clean/" + "fccb4dce-cd8b-4d64-8805-5e6a4246a40e")
    println(cpaDF.select("HOSP_ID").distinct().count())
    println(cpaERD.select("HOSPITAL_ID").distinct().count())
    cpaERD.filter($"HOSPITAL_ID".startsWith("other")).select("HOSPITAL_ID").distinct().show(false)

//    val maxDF = Parquet2DF("/test/qi/qi/max5")
//    println(maxDF.select("HOSPITAL_ID").distinct().count())
//    println(maxDF.select("PRODUCT_ID").distinct().count())
//    println(maxDF.agg(sum("f_sales")).first.get(0))
//    println(maxDF.agg(sum("f_units")).first.get(0))
//    CSV2DF("/data/nhwa/pha_config_repository1809/Nhwa_201809_Offline_MaxResult_20181126.csv").show(false)

}
