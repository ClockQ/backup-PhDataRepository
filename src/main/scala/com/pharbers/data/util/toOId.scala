package com.pharbers.data.util

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * @description:
  * @author: clock
  * @date: 2019-03-29 19:19
  */
case class toOId(id: String) {
    val oidSchema = StructType(StructField("oid", StringType, false) :: Nil)
    new GenericRowWithSchema(Array(id), oidSchema)
}
