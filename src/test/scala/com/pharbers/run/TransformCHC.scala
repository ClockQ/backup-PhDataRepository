package com.pharbers.run

/**
  * @description:
  * @author: clock
  * @date: 2019-04-16 19:24
  */
object TransformCHC extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val chcFile = "/test/OAD CHC data for 5 cities to 2018Q3 v3.csv"

    val chcDF = CSV2DF(chcFile)

    val chcCvs = CHCConversion()

    chcCvs.toERD(Map(
        "chcDF" -> chcDF
    ))

    CHC_LOCATION
}
