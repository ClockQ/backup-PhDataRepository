package com.pharbers.run

import com.pharbers.util.log.phLogTrait.phDebugLog

import scala.io.Source

object TransformMaxResult2ERD extends App {

    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util._

    val file = Source.fromFile("D:\\文件\\nhwa\\18-17").getLines()
    val sourceERD = CSV2DF("/test/dcs/source.csv")
    file.zipWithIndex.foreach(x => {
        phDebugLog("第" + x._2)
                val pfizerInfDF = Parquet2DF("/workData/Max/" + x._1)
                        .select("Date","Province","City","Panel_ID","Product","Factor","f_sales","f_units","MARKET","belong2company")

        val pfizer_source_id = "5ca069bceeefcc012918ec72"

        val pfizerInfMaxCvs = MaxResultConversion(pfizer_source_id)

        //    val pfizerInfDF = FILE2DF(pfizer_inf_csv, 31.toChar.toString)

        println("pfizerInfDF.count = " + pfizerInfDF.count())

        val maxToErdResult = pfizerInfMaxCvs.toERD(
            Map(
                "maxDF" -> pfizerInfDF,
                "sourceERD" -> sourceERD
            )
        )
        val maxERD = maxToErdResult("maxERD")


        val pfizerMinus = pfizerInfDF.count() - maxERD.count()
        phDebugLog("maxERD count = " + maxERD.count())
        assert(pfizerMinus == 0, "pfizer INF max result: 转换后的ERD比源数据减少`" + pfizerMinus + "`条记录")
        maxERD.save2Parquet("/test/dcs/maxResult_nhwa")
    })


}
