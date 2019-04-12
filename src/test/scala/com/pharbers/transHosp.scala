package com.pharbers

import com.pharbers.common.phFactory
import com.pharbers.phDataConversion.phDataHandFunc
import com.pharbers.spark.util.readParquet
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object transHosp extends App {
	val driver = phFactory.getSparkInstance()

	import driver.conn_instance

	driver.sc.addJar("target/pharbers-data-repository-1.0-SNAPSHOT.jar")
	val hospPath = "/test/hosp/"
	val addressPath = "/test/hosp/Address/"
	val repositoryPath = "/repository/"

	def trans(path: String): Unit = {
		val filePathlst = FileSystem.get(new Configuration).listStatus(new Path(path))
			.map(x => x.getPath.toString.split("/").last)
			.filter(x => x != "Address" && x != "medle")
		filePathlst.foreach { x =>
			val df = driver.setUtil(readParquet()).readParquet(path + x)
			phDataHandFunc.saveParquet(df, repositoryPath, x)
		}
	}

	trans(hospPath)
	trans(addressPath)
}
