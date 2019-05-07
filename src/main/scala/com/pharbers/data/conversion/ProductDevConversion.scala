package com.pharbers.data.conversion

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

/**
  * @description: product of calc
  * @author: clock
  * @date: 2019-04-15 14:49
  */
case class ProductDevConversion() extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._

    override def file2ERD(args: MapArgs): MapArgs = {
        val productDevERD = args.get.values.map(_.getBy[DFArgs]).reduce(_ unionByName _)
                .withColumn("DEV_PACK_ID",
                    when(col("DEV_PACK_ID").isNotNull, col("DEV_PACK_ID").cast("int")).otherwise(lit(0))
                )
                .groupBy(
                    "DEV_PRODUCT_NAME", "DEV_CORP_NAME", "DEV_MOLE_NAME",
                    "DEV_PACKAGE_DES", "DEV_PACKAGE_NUMBER", "DEV_DELIVERY_WAY", "DEV_DOSAGE_NAME"
                )
                .agg(
                    max("DEV_PACK_ID") as "DEV_PACK_ID"
                    , commonUDF.mkStringUdf(sort_array(collect_list("DEV_PACK_ID")), lit(",")) as "DEV_REPEAT_PACK_ID"
                    , commonUDF.mkStringUdf(sort_array(collect_list("DEV_SOURCE")), lit("+")) as "DEV_SOURCE"
                )
                .generateId

        MapArgs(Map(
            "productDevERD" -> DFArgs(productDevERD)
        ))
    }

    override def extractByDIS(args: MapArgs): MapArgs = ???

    override def mergeERD(args: MapArgs): MapArgs = ???

}