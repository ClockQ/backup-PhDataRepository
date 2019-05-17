package com.pharbers.data.qitest

import com.pharbers.data.run.TransformProductDev
import org.apache.spark.sql.types.IntegerType

/**
  * @description:
  * @author: clock
  * @date: 2019-04-29 13:50
  */
object devTest extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util.spark._

    import sparkDriver.ss.implicits._

    //    TransformProductDev.pfizerMatchDF.filter($"DEV_DELIVERY_WAY".isNull).show(false)
//
//    val a = // Parquet2DF(PROD_DEV_LOCATION)
//            TransformProductDev.imsPackIdDF
//            .groupBy("DEV_PRODUCT_NAME", "DEV_CORP_NAME", "DEV_MOLE_NAME",
//                "DEV_PACKAGE_DES", "DEV_PACKAGE_NUMBER", "DEV_DELIVERY_WAY", "DEV_DOSAGE_NAME")
//            .agg(sort_array(collect_list("DEV_PACK_ID")) as "DEV_PACK_ID", countDistinct($"DEV_PACK_ID") as "count")
//            .sort(col("count").desc)
//            .filter($"count" > 1)
//
//    a.show(false)
    val ym = "201809"
    val company_id: String = "5ca069bceeefcc012918ec72"

    val missHospERD = Parquet2DF("/repository/miss_hosp" + "/" + company_id).cache()
    val notPublishedHospERD = Parquet2DF("/repository/not_published_hosp" + "/" + company_id).cache()
    val fullHospERD = Parquet2DF("/repository/full_hosp" + "/" + company_id + "/20180629").cache().filter($"YM" === ym)

    val missHosp = missHospERD.filter($"DATE" === ym)
            .drop("DATE")
            .unionByName(notPublishedHospERD)
            .drop("_id")
            .distinct()

    val result = missHosp.join(fullHospERD, missHosp("HOSPITAL_ID") === fullHospERD("HOSPITAL_ID")).drop(missHosp("HOSPITAL_ID"))
    println(result.count())
    fullHospERD.filter($"HOSPITAL_ID" === "5caf917197d124239a6a9130").show(false)

//    val not_arrival_hosp = CSV2DF("hdfs:///data/nhwa/pha_config_repository1809/Missing_Hospital.csv")
//            .withColumnRenamed("MONTH", "month")
//            .filter(s"month like '%9%'")
//            .withColumnRenamed("HOSP_ID", "ID")
//            .select("ID")
//
//    val not_published_hosp = CSV2DF("hdfs:///data/nhwa/pha_config_repository1809/Nhwa_2018_NotPublishedHosp_20180629.csv")
//            .withColumnRenamed("HOSP_ID", "ID")
//
//    val miss_hosp = not_arrival_hosp.union(not_published_hosp).distinct()
//            .withColumn("ID", 'ID.cast(IntegerType))
//
//    val full_hosp_id = CSV2DF("hdfs:///data/nhwa/pha_config_repository1809/Nhwa_2018_FullHosp_20180629.csv")
//            .filter(s"MONTH == 9")
//            .withColumn("HOSP_ID", 'HOSP_ID.cast(IntegerType))
//
//    val full_hosp = miss_hosp.join(full_hosp_id, full_hosp_id("HOSP_ID") === miss_hosp("ID")).drop("ID")
////    full_hosp.select("HOSP_ID").distinct().show(100, false)
//    println(full_hosp.count())
//
//    val hospDIS = Parquet2DF(HOSP_DIS_LOCATION).dropDuplicates("PHA_HOSP_ID")
//    val phaDF = Parquet2DF(HOSP_PHA_LOCATION).dropDuplicates("CPA")
//
//    val a = result.select("HOSPITAL_ID").distinct()
//            .join(hospDIS, $"HOSPITAL_ID" === hospDIS("_id"))
//            .join(phaDF, hospDIS("PHA_HOSP_ID") === phaDF("PHA_ID_NEW"))
//                    .select("CPA")
//println(a.count())
//            a.show(100, false)
//    val b = full_hosp.select("HOSP_ID").distinct()
//            println(b.count())
//    b.show(100, false)
//
//    val c = b.join(a, a("CPA") === b("HOSP_ID"), "left")
//    c.show(100, false)

}
