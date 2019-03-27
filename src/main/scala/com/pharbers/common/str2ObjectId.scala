package com.pharbers.common

import com.pharbers.model.oidCol
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}

case class str2ObjectId(df: DataFrame) {
	val udf_struct_id: UserDefinedFunction = udf {
		oid: String => oidCol(oid)
	}
	df.withColumn("_id", udf_struct_id(col("_id")))
}
