package com.pharbers.data.qitest

/**
  * @description:
  * @author: clock
  * @date: 2019-05-15 19:19
  */
object universeOne2ManyTest extends App {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._

    import com.pharbers.data.util.spark._
    import sparkDriver.ss.implicits._

    val sourceHosp = CSV2DF("/test/2019年Universe更新维护1.0.csv")
    sourceHosp.groupBy("新版ID")
            .agg(sort_array(collect_list("新版名称")) as "新版名称", countDistinct($"新版名称") as "count")
            .sort(col("count").desc)
            .show(false)
    println(sourceHosp.dropDuplicates("新版名称").count(), sourceHosp.dropDuplicates("新版ID").count())

    val hosp = Parquet2DF(HOSP_BASE_LOCATION)
    hosp.groupBy("PHAHospId")
            .agg(sort_array(collect_list("title")) as "title", countDistinct($"title") as "count")
            .sort(col("count").desc)
            .show(false)
    println(hosp.dropDuplicates("title").count(), hosp.dropDuplicates("PHAHospId").count())
}
