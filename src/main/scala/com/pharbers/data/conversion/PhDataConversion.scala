package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

/**
  * @description: data conversion trait
  * @author: clock
  * @date: 2019-03-28 15:07
  */
trait PhDataConversion {

    def DF2ERD(args: Map[String, DataFrame]): Map[String, DataFrame]

    def ERD2DF(args: Map[String, DataFrame]): Map[String, DataFrame]
}