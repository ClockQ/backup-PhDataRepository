package com.pharbers.data.run

import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

object TransformAtcTable extends App {
    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val pfizer_cpa_csv = "/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv"
    val astellas_cpa_csv = "/test/CPA&GYCX/Astellas_201804_CPA_20180629.csv"
    val nhwa_cpa_csv = "/test/CPA&GYCX/Nhwa_201804_CPA_20181227.csv"

    val atcCvs = AtcTableConversion()

    val atcTableDF = atcCvs.toERD(MapArgs(Map(
            "pfizerCpaDF" -> DFArgs(CSV2DF(pfizer_cpa_csv))
            , "astellasCpaDF" -> DFArgs(CSV2DF(astellas_cpa_csv))
            , "nhwaCpaDF" -> DFArgs(CSV2DF(nhwa_cpa_csv))
        )
    )).getAs[DFArgs]("atcTableDF")

    phDebugLog("atcTableDF `ERD` count = " + atcTableDF.count())
    atcTableDF.show(true)

    if(args.nonEmpty && args(0) == "TRUE")
        atcTableDF.save2Parquet(PROD_ATCTABLE_LOCATION).save2Mongo(PROD_ATCTABLE_LOCATION.split("/").last)
}
