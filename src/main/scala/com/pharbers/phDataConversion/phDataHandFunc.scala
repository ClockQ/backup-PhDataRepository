package com.pharbers.phDataConversion

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.bson.types.ObjectId

object phDataHandFunc {
    val setIdCol: UserDefinedFunction = udf{
        () => ObjectId.get().toString
    }
}
