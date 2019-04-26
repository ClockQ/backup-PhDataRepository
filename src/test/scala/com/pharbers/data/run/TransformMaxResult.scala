package com.pharbers.data.run

import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

object TransformMaxResult extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val maxCvs = MaxResultConversion()

    val hospDIS = HospConversion().toDIS(MapArgs(Map(
        "hospBaseERD" -> DFArgs(Parquet2DF(HOSP_BASE_LOCATION))
        , "hospAddressERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_BASE_LOCATION))
        , "hospPrefectureERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_PREFECTURE_LOCATION))
        , "hospCityERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_CITY_LOCATION))
        , "hospProvinceERD" -> DFArgs(Parquet2DF(HOSP_ADDRESS_PROVINCE_LOCATION))
    ))).getAs[DFArgs]("hospDIS")
    val hospDISCount = hospDIS.count()

    val productDevERD = Parquet2DF(PROD_DEV_LOCATION)
    val productDevERDCount = productDevERD.count()

    maxResultTest(NHWA_COMPANY_ID, "/workData/Max/06efd8c7-5b92-4d00-87b7-30e1f655f32d")
    maxResultTest(PFIZER_COMPANY_ID, "/workData/Max/00d6ac53-cdc0-4a6c-8332-3880d715db17")

    def maxResultTest(company_id: String, max_file: String): Unit = {
        val company_id = NHWA_COMPANY_ID

        val maxDF = Parquet2DF(max_file)
        val maxDFCount = maxDF.count()
        maxDF.show(false)

        val maxERD = maxCvs.toERD(MapArgs(Map(
            "maxDF" -> DFArgs(maxDF.trim("COMPANY_ID", company_id))
            , "prodDF" -> DFArgs(productDevERD)
            , "hospDF" -> DFArgs(hospDIS)
        ))).getAs[DFArgs]("maxERD")
        val maxERDCount = maxERD.count()
        maxERD.show(false)

        val maxERDMinus = maxDFCount - maxERDCount
        phDebugLog(s"maxERDMinus : $maxDFCount - $maxERDCount = $maxERDMinus")
        assert(maxERDMinus == 0, "max: 转换后的ERD比源数据减少`" + maxERDMinus + "`条记录")

        if(args.nonEmpty && args(0) == "TRUE"){
            maxERD.save2Parquet(MAX_RESULT_LOCATION + "/" + company_id)
        }

        val maxDIS = maxCvs.toDIS(MapArgs(Map(
            "maxERD" -> DFArgs(maxERD)
            , "prodDIS" -> DFArgs(productDevERD)
            , "hospDIS" -> DFArgs(hospDIS)
        ))).getAs[DFArgs]("maxDIS")
        val maxDISCount = maxDIS.count()
        maxDIS.show(false)

        val maxDISMinus = maxDFCount - maxDISCount
        phDebugLog(s"maxDISMinus : $maxDFCount - $maxDISCount = $maxDISMinus")
        assert(maxERDMinus == 0, "max: 转换后的DIS比源数据减少`" + maxDISMinus + "`条记录")
    }
}
