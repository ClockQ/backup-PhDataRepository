package com.pharbers.phDataConversion

import com.pharbers.common.{phDataTrait, phFactory}
import com.pharbers.model.chcData
import com.pharbers.spark.phSparkDriver
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.bson.types.ObjectId

class phCHCData extends Serializable with phDataTrait {
	def getDataFromDF(df: DataFrame): Unit = {
		val rddTemp = df.na.fill("").distinct().toJavaRDD.rdd.map(x => chcData(x(0).toString, x(1).toString,
			x(2).toString, x(3).toString, x(4).toString, x(5).toString, x(6).toString, x(7).toString, x(8).toString,
			x(9).toString, x(10).toString, x(11).toString))

		val midRDD = rddTemp.map { x =>
			val chc_id = getObjectId()
			x.chc_id = chc_id
			x
		}.groupBy(x => x.date).flatMap(x => {
			val date_id = getObjectId()
			x._2.map(y => {
				y.date_id = date_id
				y
			})
		}).groupBy(x => x.city).flatMap(x => {
			val city_id = getObjectId()
			x._2.map(y => {
				y.city_id = city_id
				y
			})
		}).groupBy(x => x.sales + x.units + x.date + x.city).flatMap(x => {
			val revenue_id = getObjectId()
			x._2.map(y => {
				y.revenue_id = revenue_id
				y
			})
		}).groupBy(x => x.sales + x.units + x.date + x.city).flatMap(x => {
			val product_id = getObjectId()
			x._2.map(y => {
				y.product_id = product_id
				y
			})
		}).groupBy(x => x.packId + x.pack).flatMap(x => {
			val pack_id = getObjectId()
			x._2.map(y => {
				y.pack_id = pack_id
				y
			})
		}).groupBy(x => x.atc3 + x.oadType).flatMap(x => {
			val oad_id = getObjectId()
			x._2.map(y => {
				y.oad_id = oad_id
				y
			})
		}).groupBy(x => x.moleName + x.moleNameCN).flatMap(x => {
			val mole_id = getObjectId()
			x._2.map(y => {
				y.mole_id = mole_id
				y
			})
		}).groupBy(x => x.manufacture).flatMap(x => {
			val manufacture_id = getObjectId()
			x._2.map(y => {
				y.manufacture_id = manufacture_id
				y
			})
		}).cache()

		saveDate(midRDD)
		saveCity(midRDD)
		saveRevenue(midRDD)
		saveChc(midRDD)
		saveProduct(midRDD)
		savePack(midRDD)
		saveOad(midRDD)
		saveMole(midRDD)
		savemanufacture(midRDD)
	}

	private def saveDate(data: RDD[chcData]): Unit ={
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val rdd = data.map(x => (x.date_id, x.date, "quarter")).distinct

		phDataHandFunc.saveParquet(rdd.toDF("_id", "data", "period"), "/test/chc/", "chc-date")
	}

	private def saveCity(data: RDD[chcData]): Unit ={
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val rdd = data.map(x => (x.city_id, x.city)).distinct

		phDataHandFunc.saveParquet(rdd.toDF("_id", "city"), "/test/chc/", "chc-city")
	}

	private def saveRevenue(data: RDD[chcData]): Unit ={
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val rdd = data.map(x => (x.revenue_id, x.sales, x.units, x.date_id, x.city_id)).distinct

		phDataHandFunc.saveParquet(rdd.toDF("_id", "sales", "units", "date", "city"), "/test/chc/", "chc-revenue")
	}

	private def saveChc(data: RDD[chcData]): Unit ={
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val rdd = data.map(x => (x.chc_id, x.product_id, x.revenue_id)).distinct

		phDataHandFunc.saveParquet(rdd.toDF("_id", "prod", "revenue"), "/test/chc/", "chc-table")
	}

	private def saveProduct(data: RDD[chcData]): Unit ={
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val rdd = data.map(x => (x.product_id, x.prodName, x.pack_id, x.oad_id, x.mole_id, x.manufacture_id)).distinct

		phDataHandFunc.saveParquet(rdd.toDF("_id", "name", "pack", "oad", "mole", "manufacture"), "/test/chc/", "chc-product")
	}

	private def savePack(data: RDD[chcData]): Unit ={
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val rdd = data.map(x => (x.pack_id, x.packId, x.pack)).distinct

		phDataHandFunc.saveParquet(rdd.toDF("_id", "packId", "Pack"), "/test/chc/", "chc-pack")
	}

	private def saveOad(data: RDD[chcData]): Unit ={
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val rdd = data.map(x => (x.oad_id, x.atc3,x.oadType)).distinct

		phDataHandFunc.saveParquet(rdd.toDF("_id", "atc3", "oadType"), "/test/chc/", "chc-oad")
	}

	private def saveMole(data: RDD[chcData]): Unit ={
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val rdd = data.map(x => (x.mole_id, x.moleName, x.moleNameCN)).distinct

		phDataHandFunc.saveParquet(rdd.toDF("_id", "mole", "moleCN"), "/test/chc/", "chc-mole")
	}

	private def savemanufacture(data: RDD[chcData]): Unit ={
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val rdd = data.map(x => (x.manufacture_id, x.manufacture)).distinct

		phDataHandFunc.saveParquet(rdd.toDF("_id", "manufacture"), "/test/chc/", "chc-manufacture")
	}

	def getObjectId(): String = {
		ObjectId.get().toString
	}
}
