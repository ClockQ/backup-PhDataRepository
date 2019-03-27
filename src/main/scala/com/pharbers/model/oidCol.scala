package com.pharbers.model

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class oidCol(oid: String) {
	val PlayTimeWindow = StructType(StructField("oid", StringType, true) :: Nil)
	new GenericRowWithSchema(Array(oid), PlayTimeWindow)
}
