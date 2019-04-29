package com.pharbers.data.test

import com.pharbers.data.aggregation.CompareMaxResultAgg
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, StringArgs}

object testCompareMaxResultAgg extends App{
    import com.pharbers.reflect.PhReflect._

    CompareMaxResultAgg(
        MapArgs(
            Map(
                "sourceId" -> StringArgs("5ca069e2eeefcc012918ec73"),
                "marketMongo" -> StringArgs("marketAgg"),
                "productMongo" -> StringArgs("productAgg")
            )
        )
    ).exec()
}
