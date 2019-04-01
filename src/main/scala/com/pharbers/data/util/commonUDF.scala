package com.pharbers.data.util

import java.text.SimpleDateFormat

import org.bson.types.ObjectId
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * @description:
  * @author: clock
  * @date: 2019-03-31 21:24
  */
object commonUDF {
    val str2TimeUdf: UserDefinedFunction = udf { str: String =>
        val dateFormat = new SimpleDateFormat("yyyyMM")
        dateFormat.parse(str).getTime
    }

    val time2StrUdf: UserDefinedFunction = udf { time: Long =>
        val dateFormat = new SimpleDateFormat("yyyyMM")
        dateFormat.format(time)
    }

    val generateIdUdf: UserDefinedFunction = udf { () => ObjectId.get().toString }

    val trimOIdUdf: UserDefinedFunction = udf(toOId)
}