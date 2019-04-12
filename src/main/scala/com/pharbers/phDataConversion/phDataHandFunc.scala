package com.pharbers.phDataConversion

import com.pharbers.model.hosp.hospData
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.bson.types.ObjectId

object phDataHandFunc {
	val setIdCol: UserDefinedFunction = udf {
		() => ObjectId.get().toString
	}

	val setHospCol: UserDefinedFunction = udf {
		(_id: String, title: String, PHAIsRepeat: String, PHAHospId: String, `type`: String, level: String, character: String, addressID: String) =>
			hospData(_id, title, PHAIsRepeat, PHAHospId, `type`, level, character, addressID).toString
		//        _id: String => hospData(_id)
	}

	def getObjectID(): String = {
		ObjectId.get().toString
	}

	def saveParquet(df: DataFrame, path: String, name: String): Unit = {
		try {
			df.write.mode(SaveMode.Append)
				.option("header", value = true)
				.parquet(path + name)
		} catch {
			case ex: org.apache.spark.sql.AnalysisException => {
				println("error: " + ex)
			}
			case ex: java.lang.ArrayIndexOutOfBoundsException => {
				println("error: " + ex)
			}
		}
	}

	def string2HospData(s: String): hospData = {
		val atts = s.split(",")
		hospData(atts(0), atts(1), atts(2), atts(3), atts(4), atts(5), atts(6), atts(7))
	}
}
