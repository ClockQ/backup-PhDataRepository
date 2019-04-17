package com.pharbers.test

import com.pharbers.data.job.CPA2ERDJob

/**
  * @description:
  * @author: clock
  * @date: 2019-04-08 14:57
  */
object testCPA2ERDJob extends App {
    import com.pharbers.reflect.PhReflect._
    import com.pharbers.data.util.ParquetLocation._

    val company_id = "5ca069bceeefcc012918ec72"
    val testArgs = Map(
        "company_id" -> company_id
        , "cpa_file" -> "/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv"
        , "pha_file" -> HOSP_PHA_LOCATION
        , "hosp_base_file" -> HOSP_BASE_LOCATION
        , "prod_base_file" -> (PROD_ETC_LOCATION + "/" + company_id)
        , "prod_base_file_temp" -> "/test/qi/qi/save_prod_file"
        , "hosp_base_file_temp" -> "/test/qi/qi/save_hosp_file"
        , "pha_file_temp" -> "/test/qi/qi/save_pha_file"
    )

    println(CPA2ERDJob(testArgs)().exec)
}
