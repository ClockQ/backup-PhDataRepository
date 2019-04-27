package com.pharbers.data.test

import com.pharbers.data.job.GYCX2ERDJob

/**
  * @description:
  * @author: clock
  * @date: 2019-04-08 14:57
  */
object testGYCX2ERDJob extends App {

    import com.pharbers.reflect.PhReflect._
    import com.pharbers.data.util.ParquetLocation._

    val company_id = PFIZER_COMPANY_ID
    val testArgs = Map(
        "company_id" -> company_id
        , "gycx_file" -> "/test/CPA&GYCX/Pfizer_201804_Gycx_20181127.csv"
        , "pha_file" -> HOSP_PHA_LOCATION
        , "hosp_base_file" -> HOSP_BASE_LOCATION
        , "prod_etc_file" -> (PROD_ETC_LOCATION + "/" + company_id)
        , "hosp_base_file_temp" -> "/test/qi/qi/save_hosp_file"
        , "prod_etc_file_temp" -> "/test/qi/qi/save_prod_file"
        , "pha_file_temp" -> "/test/qi/qi/save_pha_file"
    )
    println(GYCX2ERDJob(testArgs)().exec)
}
