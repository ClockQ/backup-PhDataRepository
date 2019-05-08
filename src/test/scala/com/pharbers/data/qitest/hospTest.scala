package com.pharbers.data.qitest

import com.pharbers.data.util.Parquet2DF
import com.pharbers.data.util.ParquetLocation.HOSP_PHA_LOCATION

/**
  * @description:
  * @author: clock
  * @date: 2019-04-29 13:50
  */
object hospTest extends App {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    Parquet2DF(HOSP_PHA_LOCATION).show(false)
}
