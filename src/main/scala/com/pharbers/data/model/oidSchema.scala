package com.pharbers.data.model

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * @description:
  * @author: clock
  * @date: 2019-04-25 18:16
  */
case class oidSchema(oid: String) extends PhDataModel {
    val oidSchema = StructType(StructField("oid", StringType, nullable = false) :: Nil)
    new GenericRowWithSchema(Array(oid), oidSchema)
}
