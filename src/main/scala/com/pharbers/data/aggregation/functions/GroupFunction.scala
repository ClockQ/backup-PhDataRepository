package com.pharbers.data.aggregation.functions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object GroupFunction {
    def groupByXYGroup(group: String*): DataFrame => DataFrame = {
        df => df.withColumn("X_GROUP", col(group.head))
                .withColumn("Y_GROUP", col(group.tail.head))
    }
}
