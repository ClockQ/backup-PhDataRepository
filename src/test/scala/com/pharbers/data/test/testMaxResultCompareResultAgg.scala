package com.pharbers.data.test

import com.pharbers.data.aggregation.MaxResultCompareResultAgg
import com.pharbers.pactions.actionbase.{MapArgs, StringArgs}

object testMaxResultCompareResultAgg extends App{
    import com.pharbers.reflect.PhReflect._
    MaxResultCompareResultAgg(
        MapArgs(
            Map(
                "mongoDbSource" -> StringArgs("productCityAgg2"),
                "productMongo" -> StringArgs("productCityAggTop20"),
                "topN" -> StringArgs("20")
            )
        )
    ).exec()
}
