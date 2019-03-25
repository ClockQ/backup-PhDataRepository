package com.pharbers.phDataConversion

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, lit}

class phSpecialtyData {
	def getSpecialty(specialtyDF: DataFrame): Unit = {
		//Specialty_1	Specialty_2	Specialty_1_标准化	Specialty_2_标准化	Re-Speialty	Specialty 3
		val colList = List("_id", "title", "level", "tag", "next")
		val specialty_1 = addCols(specialtyDF, 0, "specialty_1", 0, "标准化前", "specialty_2", colList)
		val specialty_2 = addCols(specialtyDF, 1, "Specialty_2", 1, "标准化前", "specialty_3", colList)
		val specialty_3 = addCols(specialtyDF, 2, "Specialty 3", 2, "标准化前", "next_empty", colList)
		val specialty_1_standard = addCols(specialtyDF, 3, "Specialty_1_标准化", 0, "Specialty_2_标准化", "specialty_2", colList)
		val specialty_2_standard = addCols(specialtyDF, 4, "Specialty_2_标准化", 1, "next_empty", "specialty_2", colList)
		val specialty_re = addCols(specialtyDF, 5, "specialty_re", 0, "re", "next_empty", colList)
		val dfList = List(specialty_1, specialty_2, specialty_3, specialty_1_standard, specialty_2_standard, specialty_re)
		dfList.foreach(x => saveSpecialty(x, "/test/testAddress/specialty"))
	}

	def addCols(specialtyDF: DataFrame, idIndex: Int, specialtyName: String, level: Int, tag: String, next: String, colList: List[String]): DataFrame = {
		specialtyDF.withColumnRenamed(specialtyName, "title")
    		.withColumn("_id", lit(col("_id").asInstanceOf[Seq[String]])(idIndex))
    		.withColumn("next_empty", lit(""))
    		.withColumn("level", lit(level))
    		.withColumn("tag", lit(tag))
    		.withColumn("next", col(next))
    		.select(colList.head, colList.tail: _*)
	}

	def saveSpecialty(speResultDF: DataFrame, path: String): Unit ={
		speResultDF.write.mode(SaveMode.Append)
			.option("header", value = true)
			.parquet(path)
	}
}
