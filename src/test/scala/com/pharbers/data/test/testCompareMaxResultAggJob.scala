package com.pharbers.data.test

import com.pharbers.data.job.aggregationJob.CompareMaxResultAgg

object testCompareMaxResultAggJob extends App{
    import com.pharbers.reflect.PhReflect._
    import com.pharbers.data.util.ParquetLocation._

    CompareMaxResultAgg(
        Map(
            "sourceId" -> "5ca069e2eeefcc012918ec73"
        )
    ).exec()
}
