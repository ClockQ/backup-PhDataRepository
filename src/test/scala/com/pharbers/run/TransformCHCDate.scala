package com.pharbers.run

/**
  * @description:
  * @author: clock
  * @date: 2019-04-16 19:24
  */
object TransformCHCDate extends App {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    val chcFile = "/test/OAD CHC data for 5 cities to 2018Q3 v3.csv"
    val chcDF = CSV2DF(chcFile)

    val oldDateDF = try {
        Parquet2DF(CHC_DATE_LOCATION)
    } catch {
        case _: Exception => Seq.empty[(String, String, String)].toDF("_id", "TIME", "PERIOD")
    }

    val newDateDF = chcDF.select("Date")
            .distinct()
            .withColumn("PERIOD", lit("quarter"))
            .join(oldDateDF, chcDF("Date") === oldDateDF("TIME"), "left")
            .filter(col("_id").isNull)
            .drop(oldDateDF("_id"))
            .drop(oldDateDF("TIME"))
            .drop(oldDateDF("PERIOD"))
            .select(col("Date").as("TIME"), col("PERIOD"))
            .generateId

    newDateDF.show(false)

    if (args.isEmpty || args(0) == "TRUE") {
        newDateDF.save2Parquet(CHC_DATE_LOCATION)
        newDateDF.save2Mongo(CHC_DATE_LOCATION.split("/").last)
    }
}
