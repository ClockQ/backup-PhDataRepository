package com.pharbers.model

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class oidCol(oid: String) {
	val oidSchema = StructType(StructField("oid", StringType, false) :: Nil)
	new GenericRowWithSchema(Array(oid), oidSchema)
}
