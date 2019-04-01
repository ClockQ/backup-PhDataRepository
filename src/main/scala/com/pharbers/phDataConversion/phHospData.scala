package com.pharbers.phDataConversion

import com.pharbers.spark.phSparkDriver
import com.pharbers.model.hosp._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import com.pharbers.common.phFactory


class phHospData() extends Serializable {
	val path = "/repository/"
	def getHospDataFromCsv(df: DataFrame): Unit = {
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val rmbId = phDataHandFunc.getObjectID()
		phDataHandFunc.saveParquet(List((rmbId, "人民币", 1)).toDF("_id", "title", "rate"), path, "unit")
		val data = df.withColumn("hospId", phDataHandFunc.setIdCol())
			.na.fill("")
			.cache()

		val hosp = List("hospId", "新版名称", "新版ID", "Type", "Hosp_level", "性质", "addressId")
		val numMap = Map("outpatient" -> List("年诊疗人次", "内科诊次", "内科诊次", "外科诊次", "入院人数", "住院病人手术人次数"),
			"bed" -> List("床位数", "全科床位数", "内科床位数", "外科床位数", "眼科床位数"),
			"revenue" -> List("医疗收入", "门诊收入", "门诊治疗收入", "门诊手术收入", "住院收入", "住院床位收入", "住院治疗收入", "住院手术收入", "药品收入", "门诊药品收入", "门诊西药收入", "住院药品收入", "住院西药收入"),
			"staffNum" -> List("医生数", "在职员工人数"))

		var hospRDD = getEstimate(data, "2019") union getSpecialty(data)

		numMap.foreach { case (name, list) =>
			hospRDD = hospRDD union getNumbers(getRdd(hosp, numMap(name), data), "2019", name, list)(hospSetNumberId(name), numbersToDf(name, rmbId))
		}
		val hospDF = hospRDD.keyBy(x => x._id).reduceByKey((left, right) => {
			left.revenues = (left.revenues ::: right.revenues).distinct
			left.nobs = (left.nobs ::: right.nobs).distinct
			left.estimates = (left.estimates ::: right.estimates).distinct
			left.noo = (left.noo ::: right.noo).distinct
			left.nos = (left.nos ::: right.nos).distinct
			left.specialty = (left.specialty ::: right.specialty).distinct
			left
		}).map(x => x._2).toDF("_id", "title", "PHAHospId", "type", "level", "character", "addressID", "nos", "estimates", "noo", "nobs", "revenues", "specialty")
		phDataHandFunc.saveParquet(hospDF, path, "hosp")
	}

	def getRdd(hosp: List[String], numbers: List[String], df: DataFrame): RDD[data] = {
		df.select(hosp.head, hosp.tail ::: numbers: _*).javaRDD.rdd.map(x =>

			data(hospData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString, x(5).toString, x(6).toString),
				collection.mutable.ArrayBuffer.apply(numbers.map(y => (x(numbers.indexOf(y) + 7).toString, "", "")): _*)))
	}


	def getEstimate(df: DataFrame, tag: String): RDD[hospData] = {
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val res = df.select("hospId", "新版名称", "新版ID", "Type", "Hosp_level", "性质", "addressId", "Est_DrugIncome_RMB")
			.javaRDD.rdd.map(x => (hospData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString, x(5).toString, x(6).toString), x(7).toString))
			.groupBy(x => x._2)
			.flatMap(x => {
				val id = phDataHandFunc.getObjectID()
				x._2.map(y => {
					y._1.estimates = List(id)
					(y._1, y._2, id)
				})
			}).cache()

		phDataHandFunc.saveParquet(res.map(x => (x._3, "Est_DrugIncome_RMB", "tag", tryToInt(x._2))).distinct().toDF("_id", "title", "tag", "amount"),
			path, "estimate")

		res.map(x => x._1)
	}

	def getSpecialty(df: DataFrame): RDD[hospData] = {
		val specialtyNameList = List("Specialty_1", "Specialty_2", "Specialty_1_标准化", "Specialty_2_标准化", "Re-Speialty", "Specialty 3")
		val hosp = List("hospId", "新版名称", "新版ID", "Type", "Hosp_level", "性质", "addressId")
		var rdd = df.select(hosp.head, hosp.tail ::: specialtyNameList: _*)
			.javaRDD.rdd.map(x => (hospData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString, x(5).toString, x(6).toString),
			specialty(x(7).toString, x(8).toString, x(9).toString, x(10).toString, x(11).toString, x(12).toString)))

		def groupSpecialty(rdd: RDD[(hospData, specialty)], name: String): RDD[(hospData, specialty)] = {
			val groupTypeMap: Map[String, specialty => String] = Map(
				"Specialty_1" -> (specialty => specialty.Specialty_1 + specialty.Specialty_2 + specialty.Specialty3),
				"Re-Speialty" -> (specialty => specialty.Re_Speialty),
				"Specialty 3" -> (specialty => specialty.Specialty3),
				"Specialty_1_标准化" -> (specialty => specialty.Specialty_1_sta + specialty.Specialty_2_sta),
				"Specialty_2" -> (specialty => specialty.Specialty_2 + specialty.Specialty3),
				"Specialty_2_标准化" -> (specialty => specialty.Specialty_2_sta))
			val firstSpecialty = rdd.groupBy(x => groupTypeMap(name)(x._2)).flatMap(x => {
				val id = phDataHandFunc.getObjectID()
				x._2.map(y => {
					y._2.idList = y._2.idList :+ id
					y._1.specialty = y._1.specialty :+ id
					y
				})
			}).cache()

			val otherSpecialty = rdd.groupBy(x => groupTypeMap(name)(x._2)).flatMap(x => {
				val id = phDataHandFunc.getObjectID()
				x._2.map(y => {
					y._2.idList = y._2.idList :+ id
					y
				})
			}).cache()

			Map("Specialty_1" -> firstSpecialty, "Specialty_2" -> otherSpecialty, "Specialty_1_标准化" -> firstSpecialty,
				"Re-Speialty" -> firstSpecialty, "Specialty 3" -> otherSpecialty, "Specialty_2_标准化" -> otherSpecialty
			)(name)
		}

		specialtyNameList.foreach(x => rdd = groupSpecialty(rdd, x))
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		new phSpecialtyData().getSpecialty(rdd.map(x => x._2).toDF("Specialty_1", "Specialty_2", "Specialty_1_标准化", "Specialty_2_标准化", "Re-Speialty", "Specialty 3", "_id"))
		rdd.map(x => x._1)
	}

	def getNumbers(rdd: RDD[data], tag: String, name: String, titleList: List[String])
	              (func: (String, hospData) => hospData, toDfFunc: RDD[data] => String => DataFrame): RDD[hospData] = {

		var rddData = rdd
		titleList.zipWithIndex.foreach { case (title, index) =>
			rddData = rddData.groupBy(x => x.array(index)._1)
				.flatMap(x => {
					val id = phDataHandFunc.getObjectID()
					x._2.map(y => {
						y.array(index) = (y.array(index)._1, title, id)
						data(func(id, y.hosp), y.array)
					})
				}).cache()
		}

		phDataHandFunc.saveParquet(toDfFunc(rddData)(tag), path, name)
		rddData.map(x => x.hosp)
	}

	def numbersToDf(name: String, rmbId: String = "")(rddData: RDD[data])(tag: String): DataFrame = {
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._
		Map("revenue" -> rddData.map(x => x.array).flatMap(x => x.map(y => (tryToInt(y._1), y._2, y._3, y._2 + tag, 365, rmbId))).toDF("amount", "title", "_id", "tag", "period", "unit")
		).getOrElse(name, rddData.map(x => x.array).flatMap(x => x.map(y => (tryToInt(y._1), y._2, y._3, y._2 + tag))).toDF("amount", "title", "_id", "tag"))

	}

	def hospSetNumberId(name: String): (String, hospData) => hospData = {
		val a: Map[String, (String, hospData) => hospData] = Map("outpatient" -> ((id, hosp) => {
			hosp.noo = (hosp.noo :+ id).distinct; hosp
		}),
			"bed" -> ((id, hosp) => {
				hosp.nobs = (hosp.nobs :+ id).distinct; hosp
			}),
			"revenue" -> ((id, hosp) => {
				hosp.revenues = (hosp.revenues :+ id).distinct; hosp
			}),
			"staffNum" -> ((id, hosp) => {
				hosp.nos = (hosp.nos :+ id).distinct; hosp
			})
		)
		a(name)
	}

	def tryToInt(string: String): Int = {
		"\\d+".r.findFirstIn(string.trim) match {
			case s: Some[String] => s.get.toInt
			case _ => 0
		}
	}
}

case class data(hosp: hospData, array: collection.mutable.ArrayBuffer[(String, String, String)])
