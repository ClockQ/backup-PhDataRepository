package com.pharbers

import com.pharbers.common.phFactory
import com.pharbers.spark.util.readCsv

object maxResultTest extends App {
	val driver = phFactory.getSparkInstance()

	import driver.conn_instance

	driver.sc.addJar("target/pharbers-data-repository-1.0-SNAPSHOT.jar")

	val panel_dvp = "/workData/Panel/b21a8ba4-07b4-dcd5-d871-344522229773"
	val max_dvp = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-DVP.csv"
	val panel_CNS_R = "/workData/Panel/54eba222-c338-71cd-fbd3-e34d8f188b05"
	val max_CNS_R = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-CNS_R.csv"

	def saveCsv(inPath: String, name: String): Unit ={
		val df = driver.setUtil(readCsv()).readCsv(inPath, 31.toChar.toString)
		df.coalesce(1).write
			.format("csv")
			.option("encoding", "UTF-8")
			.option("header", value = true)
			.option("delimiter", "#")
			.save("/test/panelResult/" + name)
	}

	val nameList = List("201804_DVP_panel_result", "201804_DVP_max_result", "201804_CNS_R_panel_result", "201804_CNS_R_max_result")
	val pathList = List(panel_dvp, max_dvp, panel_CNS_R, max_CNS_R)

	pathList.foreach(x => saveCsv(x, nameList(pathList.indexOf(x))))

}

