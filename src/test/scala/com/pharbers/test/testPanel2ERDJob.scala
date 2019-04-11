package com.pharbers.test

object testPanel2ERDJob extends App {
    import com.pharbers.data.util._
    import com.pharbers.data.util.ParquetLocation._
    var df = Parquet2DF(HOSP_BASE_LOCATION)
//    var df2 = Parquet2DF(PFIZER_CPA_LOCATION)
    df.show(false)
}
