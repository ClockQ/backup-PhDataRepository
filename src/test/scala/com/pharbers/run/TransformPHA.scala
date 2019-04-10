package com.pharbers.run

import com.pharbers.data.util.{CSV2DF, SaveParquet}
import com.pharbers.data.util.ParquetLocation.HOSP_PHA_LOCATION

/**
  * @description:
  * @author: clock
  * @date: 2019-04-08 16:57
  */
object TransformPHA extends App {
    val pha_csv = "/test/CPA&GYCX/CPA_GYC_PHA.csv"
    val phaDF = CSV2DF(pha_csv).withColumnRenamed("PHA ID", "PHA_ID")
    phaDF.save2Parquet(HOSP_PHA_LOCATION)
}
