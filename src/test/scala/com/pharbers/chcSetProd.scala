package com.pharbers

import com.pharbers.common.phFactory
import com.pharbers.spark.phSparkDriver
import com.pharbers.spark.util.{readCsv, readParquet}
import org.apache.spark.sql.functions.col

object chcSetProd extends App {
	lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()

	import sparkDriver.ss.implicits._
	import sparkDriver.conn_instance

	val path1 = "/test/chcAndprod/"
	val path2 = ""

	val atc = "cn_atc_ref_201812_1.txt"
	val mnf = "cn_mnf_ref_201812_1.txt"
	val mol_lkp = "cn_mol_lkp_201812_1.txt"
	val mol_ref = "cn_mol_ref_201812_1.txt"
	val prod_ref = "cn_prod_ref_201812_1.txt"

	val mnfDF = sparkDriver.setUtil(readCsv()).readCsv(path1 + mnf)
	val prodDF = sparkDriver.setUtil(readCsv()).readCsv(path1 + prod_ref)
	mnfDF.withColumnRenamed("MNF_ID", "MNF_ID_1")
    		.join(prodDF, col("MNF_ID") === col("MNF_ID_1"))
    		.select("MNF_ID")

	mnfDF.show(false)
	prodDF.show(false)
	mnfDF.count()
}
