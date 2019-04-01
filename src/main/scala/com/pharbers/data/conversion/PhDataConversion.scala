package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

/**
  * @description: data conversion trait
  * @author: clock
  * @date: 2019-03-28 15:07
  */
trait PhDataConversion {

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame]

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame]
}