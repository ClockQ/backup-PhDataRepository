package com.pharbers.data.test

object testPanel2ERDJob extends App {
    import com.pharbers.data.util._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util.spark._
    var df = Parquet2DF(HOSP_BASE_LOCATION)
//    var df2 = Parquet2DF(PFIZER_CPA_LOCATION)
    df.show(false)
}
