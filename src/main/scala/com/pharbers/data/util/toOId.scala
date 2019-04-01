package com.pharbers.data.util

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * @description:
  * @author: clock
  * @date: 2019-04-01 10:32
  */
case class toOId(id: String) {
    val oidSchema = StructType(StructField("oid", StringType, false) :: Nil)
    new GenericRowWithSchema(Array(id), oidSchema)
}
