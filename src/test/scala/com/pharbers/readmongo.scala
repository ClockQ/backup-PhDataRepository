package com.pharbers

import com.pharbers.common.phFactory
import com.pharbers.spark.util.mongo2RDD

object readmongo extends App {
	val phSparkDriver = phFactory.getSparkInstance()

	import phSparkDriver.conn_instance

	val rdd = phSparkDriver.setUtil(mongo2RDD()).mongo2RDD("192.168.100.176", "27017", "pharbers-hospital-complete", "Pfizer")
//	rdd.take(10).foreach(println(_))
	val df = rdd.toDF()
	df.show(false)
	df.printSchema()
//	df.count()
//	df.show(false)
}
