package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class CPAConversion() extends PhDataConversion {
    import com.pharbers.data.util._
    sparkDriver.sc.addJar("target/pharbers-data-repository-1.0-SNAPSHOT.jar")

    val pfizer_cpa = CSV2DF("/test/CPA&GYCX/Pfizer_201804_CPA_20181227.csv")
    pfizer_cpa.show(true)

    def DF2ERD(args: Map[String, DataFrame]): Map[String, DataFrame] = ???
    def ERD2DF(args: Map[String, DataFrame]): Map[String, DataFrame] = ???
}
