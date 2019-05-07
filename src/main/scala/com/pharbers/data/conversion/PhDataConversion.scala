package com.pharbers.data.conversion

import com.pharbers.pactions.actionbase.MapArgs

/**
  * @description: data conversion trait
  * @author: clock
  * @date: 2019-03-28 15:07
  */
trait PhDataConversion {

    def file2ERD(args: MapArgs): MapArgs

    def extractByDIS(args: MapArgs): MapArgs

    def mergeERD(args: MapArgs): MapArgs
}