package com.pharbers

import com.mongodb.spark.sql.fieldTypes.ObjectId
import com.mongodb.spark.sql.helpers.StructFields
import com.pharbers.common.phFactory
import com.pharbers.model.oidCol
import com.pharbers.spark.util.{dataFrame2Mongo, mongo2RDD, readParquet}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.bson.types

object mongoOITest extends App with Serializable {
	val phSparkDriver = phFactory.getSparkInstance()

	import phSparkDriver.conn_instance

	val PlayTimeWindow = StructType(StructField("oid", StringType, true) :: Nil)

	phSparkDriver.sc.addJar("/Users/cui/github/PhDataRepository/target/pharbers-data-repository-1.0-SNAPSHOT.jar")
	phSparkDriver.sc.addJar("/Users/cui/github/micro-service-libs/spark_driver/target/spark_driver-1.0.jar")
	val globalizedPlayTimeWindows =
		StructType(
			StructFields.objectId("_id", nullable = false) ::
				StructField("tag", StringType, nullable = true) ::
				StructField("tag1", StringType, nullable = true) ::
				StructFields.objectId("_id_1", nullable = false) ::
				Nil)

	val udf_struct_id: UserDefinedFunction = udf {
		oid: String => oidCol(oid)
	}

	val df_test = phSparkDriver.setUtil(readParquet()).readParquet("/test/prod/test")
	val result_test_df = df_test.withColumn("_id_1", udf_struct_id(col("auth_id")))
	phSparkDriver.setUtil(dataFrame2Mongo()).dataFrame2Mongo(result_test_df, "192.168.100.176", "27017", "pharbers-hospital-complete", "test-cui-1")
	result_test_df.write.mode(SaveMode.Append)
		.option("header", value = true)
		.parquet("/test/hosp/test-cui-1")
//	val settier: UserDefinedFunction = udf{
//		() => types.ObjectId.get()
//	}

	//	val rdd = phSparkDriver.setUtil(mongo2RDD()).mongo2RDD("192.168.100.176", "27017", "pharbers-hospital-complete", "bed")
	//	val rdd1 = rdd.rdd.map(x => Row(x.get("amount"), x.get("title"), x.get("_id"), x.get("tag")))
	//	val rdd1 = df.toJavaRDD.rdd.map(x => Row(x(0).toString, x(1).toString, ObjectId(x(2).toString), x(3).toString))
	//	rdd1.foreach(println(_))

//	sucess test
	val rdd1 = phSparkDriver.setUtil(readParquet()).readParquet("/test/prod/test").rdd
		.map(x => Row(x(0), x(1), x(2), new GenericRowWithSchema(Array(x(1)), PlayTimeWindow)))
	val result = phSparkDriver.sqc.createDataFrame(rdd1, globalizedPlayTimeWindows)
	result.show(false)
	phSparkDriver.setUtil(dataFrame2Mongo()).dataFrame2Mongo(result, "192.168.100.176", "27017", "pharbers-hospital-complete", "test-cui")
	result.write.mode(SaveMode.Append)
		.option("header", value = true)
		.parquet("/test/hosp/test-cui")

	//	rdd.take(20).foreach(println(_))
	//	val df1 = phSparkDriver.sqc.createDataFrame(rdd, globalizedPlayTimeWindows)

	//	df1.count()
	//	df1.show(false)

	//	val rdd1 = phSparkDriver.ss.read.option("header", "true").parquet("/test/hosp/bed").rdd
	//	rdd1.take(10).foreach(println(_))


	//	val phSparkDriver = phFactory.getSparkInstance()
	//
	//	import phSparkDriver.conn_instance
	//
	//	phSparkDriver.sc.addJar("/Users/cui/github/PhDataRepository/target/pharbers-data-repository-1.0-SNAPSHOT.jar")
	//	val globalizedPlayTimeWindows =
	//		StructType(
	//			StructField("MOH", StringType, true) ::
	//				StructField("Servier_DDI_Name", StringType, true) ::
	//				StructField("_id", PlayTimeWindow, true) ::
	//				StructField("等级", StringType, true) ::
	//				Nil)
	//
	//	val PlayTimeWindow = StructType(Seq(StructField("oid", StringType, true)))
	//	val rdd = phSparkDriver.setUtil(mongo2RDD()).mongo2RDD("192.168.100.176", "27017", "pharbers-hospital-complete", "bed")
	////	val r1 = rdd.map(x => x("_id"))
	//	rdd.take(20).foreach(println(_))
}
