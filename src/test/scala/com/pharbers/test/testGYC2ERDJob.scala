package com.pharbers.test

import com.pharbers.data.job.GYC2ERDJob

/**
  * @description:
  * @author: clock
  * @date: 2019-04-08 14:57
  */
object testGYC2ERDJob extends App {
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.reflect.PhReflect._

    val testArgs = Map(
        "company_id" -> "5ca069bceeefcc012918ec72"
        , "gyc_file" -> "/test/CPA&GYCX/Astellas_201804_Gycx_20180703.csv"
        , "pha_file" -> HOSP_PHA_LOCATION
        , "hosp_base_file" -> HOSP_BASE_LOCATION
        , "prod_base_file" -> PROD_BASE_LOCATION
        , "prod_delivery_file" -> PROD_DELIVERY_LOCATION
        , "prod_dosage_file" -> PROD_DOSAGE_LOCATION
        , "prod_mole_file" -> PROD_MOLE_LOCATION
        , "prod_package_file" -> PROD_PACKAGE_LOCATION
        , "prod_corp_file" -> PROD_CORP_LOCATION
        , "save_prod_file" -> "/test/qi/qi/save_prod_file"
        , "save_hosp_file" -> "/test/qi/qi/save_hosp_file"
        , "save_pha_file" -> "/test/qi/qi/save_pha_file"
    )

    println(GYC2ERDJob(testArgs).exec)
}
