package com.pharbers.data.qitest

import com.pharbers.data.run.TransformProductDev

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

    TransformProductDev.pfizerMatchDF.filter($"DEV_DELIVERY_WAY".isNull).show(false)

    val a = // Parquet2DF(PROD_DEV_LOCATION)
            TransformProductDev.imsPackIdDF
            .groupBy("DEV_PRODUCT_NAME", "DEV_CORP_NAME", "DEV_MOLE_NAME",
                "DEV_PACKAGE_DES", "DEV_PACKAGE_NUMBER", "DEV_DELIVERY_WAY", "DEV_DOSAGE_NAME")
            .agg(sort_array(collect_list("DEV_PACK_ID")) as "DEV_PACK_ID", countDistinct($"DEV_PACK_ID") as "count")
            .sort(col("count").desc)
            .filter($"count" > 1)

    a.show(false)
}
