package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

/**
  * @description: product of IMS
  * @author: clock
  * @date: 2019-04-15 14:47
  */
case class IMSProductConversion() extends PhDataConversion {

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        ???
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = ???
}
