package com.pharbers.data.conversion

import com.pharbers.spark.phSparkDriver
import com.pharbers.pactions.actionbase.MapArgs

/**
  * @description: data conversion trait
  * @author: clock
  * @date: 2019-03-28 15:07
  */
trait PhDataConversion {
    implicit val sparkDriver: phSparkDriver

    def toERD(args: MapArgs): MapArgs

    def toDIS(args: MapArgs): MapArgs
}