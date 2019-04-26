package com.pharbers.data.run

/**
  * @description:
  * @author: clock
  * @date: 2019-04-08 16:57
  */
object TransformPHA extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    val pha_csv = "/test/CPA&GYCX/CPA_GYC_PHA.csv"

    val phaDF = CSV2DF(pha_csv)

    val oldPhaDF = try {
        Parquet2DF(HOSP_PHA_LOCATION)
    } catch {
        case _: Exception => Seq.empty[(String, String, String, String)].toDF("CPA", "GYC", "PHA_ID", "PHA_ID_NEW")
    }

    val newPhaDF = phaDF
            .withColumnRenamed("PHA ID", "PHA_ID")
            .unionByName(oldPhaDF)
            .distinct()

    newPhaDF.show(false)

    if(args.nonEmpty && args(0) == "TRUE"){
        newPhaDF.save2Parquet(HOSP_PHA_LOCATION)
    }
}
