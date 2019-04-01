package com.pharbers.phDataConversion

import com.pharbers.common.phFactory
import com.pharbers.spark.phSparkDriver
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, lit}

class phSpecialtyData {
    def getSpecialty(specialtyDF: DataFrame): Unit = {
        //Specialty_1	Specialty_2	Specialty_1_标准化	Specialty_2_标准化	Re-Speialty	Specialty 3
        val colList = List("_id", "title", "level", "tag", "next")
        val specialty_1 = addCols(specialtyDF, 0, "specialty_1", 0, "标准化前", colList, 1, "nextId")
        val specialty_2 = addCols(specialtyDF, 1, "Specialty_2", 1, "标准化前", colList, 5, "nextId")
        val specialty_3 = addCols(specialtyDF, 2, "Specialty 3", 2, "标准化前", colList, 0, "entryNextId")
        val specialty_1_standard = addCols(specialtyDF, 3, "Specialty_1_标准化", 0, "specialty_1", colList, 3, "nextId")
        val specialty_2_standard = addCols(specialtyDF, 4, "Specialty_2_标准化", 1, "specialty_2", colList, 0, "entryNextId")
        val specialty_re = addCols(specialtyDF, 5, "Re-Speialty", 0, "re", colList, 0, "entryNextId")
        val dfList = List(specialty_1, specialty_2, specialty_3, specialty_1_standard, specialty_2_standard, specialty_re)
        dfList.foreach(x => phDataHandFunc.saveParquet(x, "/test/hosp/", "specialty"))
    }

    private def addCols(specialtyDF: DataFrame, idIndex: Int, specialtyName: String, level: Int, tag: String, colList: List[String], nextIdInext: Int, nextIdFlag: String): DataFrame = {
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._
        val entryNextFun: Int => DataFrame = nextIndex => {
            specialtyDF.withColumn("next", lit(""))
        }
        val nextFun: Int => DataFrame = nextIndex => {
            specialtyDF.withColumn("next", lit(col("_id").as[Array[String]])(nextIndex))
        }
        val func_map = Map("nextId" -> nextFun, "entryNextId" -> entryNextFun)
        func_map(nextIdFlag)(nextIdInext).withColumnRenamed(specialtyName, "title")
                .withColumn("_id", lit(col("_id").as[Array[String]])(idIndex))
                .withColumn("level", lit(level))
                .withColumn("tag", lit(tag))
                .select(colList.head, colList.tail: _*)
                .distinct()
    }
}
