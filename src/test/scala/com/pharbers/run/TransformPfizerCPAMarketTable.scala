package com.pharbers.run

import com.pharbers.util.log.phLogTrait.phDebugLog

object TransformPfizerCPAMarketTable extends App {

    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util._

    val pfizer_cpa_csv = "/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv"
    val astellas_cpa_csv = "/test/CPA&GYCX/Astellas_201804_CPA_20180629.csv"
    val nhwa_cpa_csv = "/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv"

    val atcCvs = AtcTableConversion()

    val atcTableDF = atcCvs.toERD(
        Map(
            "pfizerCpaDF" -> CSV2DF(pfizer_cpa_csv),
            "astellasCpaDF" -> CSV2DF(astellas_cpa_csv),
            "nhwaCpaDF" -> CSV2DF(nhwa_cpa_csv)
        )
    )("atcTableDF")

    phDebugLog("atcTableDF coount = " + atcTableDF.count())
    atcTableDF.show(true)

    atcTableDF.save2Parquet(PROD_ATCTABLE_LOCATION)
    atcTableDF.save2Mongo(PROD_ATCTABLE_LOCATION.split("/").last)
}