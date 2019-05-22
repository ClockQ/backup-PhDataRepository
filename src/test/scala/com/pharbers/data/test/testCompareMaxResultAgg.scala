package com.pharbers.data.test

import com.pharbers.data.aggregation.CompareMaxResultAgg
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs, StringArgs}

object testCompareMaxResultAgg extends App{
    import com.pharbers.reflect.PhReflect._
    CompareMaxResultAgg(
        MapArgs(
            Map(
                "sourceId" -> StringArgs("5ca069e2eeefcc012918ec73"),
                "addressType" -> StringArgs("CITY"),
                "ymType" -> StringArgs("YTD"),
                "marketMongo" -> StringArgs("marketCityAgg2"),
                "productMongo" -> StringArgs("productCityAgg2"),
//                "maxAgg" -> StringArgs("/repository/agg/maxResult/city2")
                "maxAgg" -> StringArgs("/test/dcs/agg/maxResult/city"),
                "market" -> StringArgs("/repository/agg/maxResult/Pfizer_MarketMatchTable.csv")
            )
        )
    ).exec()
}
