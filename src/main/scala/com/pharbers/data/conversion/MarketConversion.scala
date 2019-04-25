package com.pharbers.data.conversion

import com.pharbers.pactions.actionbase.MapArgs

/**
  * @description:
  * @author: clock
  * @date: 2019-04-18 17:09
  */
case class MarketConversion() extends PhDataConversion {

    import com.pharbers.data.util.sparkDriver.ss.implicits._

    override def toERD(args: MapArgs): MapArgs = ???

    override def toDIS(args: MapArgs): MapArgs = ???
}
