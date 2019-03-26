package com.pharbers.common

import com.pharbers.spark.util.{dataFrame2Mongo, readParquet}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class savePath2Mongo extends connectMongoInfo {
	val phSparkDriver = phFactory.getSparkInstance()

	import phSparkDriver.conn_instance

	def saveDF(path: String): Unit = {
		val filePathlst = FileSystem.get(new Configuration).listStatus(new Path(path))
			.map(x => x.getPath.toString.split("/").last)
		filePathlst.foreach { x =>
			val df = phSparkDriver.setUtil(readParquet()).readParquet(path + x)
			phSparkDriver.setUtil(dataFrame2Mongo()).dataFrame2Mongo(df, server_host, server_port.toString,
				conn_name, x)
		}
	}
}
