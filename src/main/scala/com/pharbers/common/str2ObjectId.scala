package com.pharbers.common

import com.pharbers.model.oidCol
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.bson.types.ObjectId

case class str2ObjectId() {
	def reset(df: DataFrame): DataFrame = {
		val udf_struct_id: UserDefinedFunction = udf {
			oid: String => oidCol(ObjectId.get().toString)
		}
		df.withColumn("_id", udf_struct_id(col("_id")))
	}
}
