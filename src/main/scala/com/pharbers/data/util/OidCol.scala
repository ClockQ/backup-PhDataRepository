package com.pharbers.data.util

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.bson.types.ObjectId

/**
  * @description:
  * @author: clock
  * @date: 2019-03-29 16:02
  */
// 不需要了，暂时保留
case class OidCol() {
    val oidSchema = StructType(StructField("oid", StringType, nullable = false) :: Nil)
    new GenericRowWithSchema(Array(ObjectId.get().toString), oidSchema)
}
