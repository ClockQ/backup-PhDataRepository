package com.pharbers.phDataConversion

import com.pharbers.common.phDataTrait
import com.pharbers.data.util.getSparkDriver
import com.pharbers.model.chcData
import com.pharbers.spark.phSparkDriver
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.bson.types.ObjectId
import com.pharbers.data.util._

class phCHCData extends Serializable with phDataTrait {
	lazy val sparkDriver: phSparkDriver = getSparkDriver()

	import sparkDriver.ss.implicits._

	def getDataFromDF(df: DataFrame): Unit = {
		val rddTemp = df.na.fill("").distinct().toJavaRDD.rdd.map(x => chcData(x(0).toString, x(1).toString,
			x(2).toString, x(3).toString, x(4).toString, x(5).toString, x(6).toString, x(7).toString, x(8).toString,
			x(9).toString, x(10).toString, x(11).toString))

		val midRDD = rddTemp.distinct().map { x =>
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
		}).groupBy(x => x.sales + x.units).flatMap(x => {
			val revenue_id = getObjectId()
			x._2.map(y => {
				y.revenue_id = revenue_id
				y
			})
		}).groupBy(x => x.prodName + x.packId + x.pack + x.moleName + x.moleNameCN + x.manufacture).flatMap(x => {
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

	private def saveDate(data: RDD[chcData]): Unit = {
		val rdd = data.map(x => (x.date_id, x.date, "quarter")).distinct
		rdd.take(20).foreach(println(_))
		rdd.toDF("_id", "data", "period").save2Parquet("/test/chc/chcDate")
	}

	private def saveCity(data: RDD[chcData]): Unit = {
		val rdd = data.map(x => (x.city_id, x.city)).distinct
		rdd.take(20).foreach(println(_))
		rdd.toDF("_id", "city").save2Parquet("/test/chc/chcCity")
	}

	private def saveRevenue(data: RDD[chcData]): Unit = {
		val rdd = data.map(x => (x.revenue_id, x.sales, x.units)).distinct
		rdd.toDF("_id", "sales", "units").save2Parquet("/test/chc/chcRevenue")
	}

	private def saveChc(data: RDD[chcData]): Unit = {
		val rdd = data.map(x => (x.chc_id, x.product_id, x.revenue_id, x.date_id, x.city_id, x.oad_id)).distinct
		rdd.toDF("_id", "prod_id", "revenue_id", "date_id", "city_id", "oad_id").save2Parquet("/test/chc/chcTable")
	}

	private def saveProduct(data: RDD[chcData]): Unit = {
		val rdd = data.map(x => (x.product_id, x.prodName, x.pack_id, x.mole_id, x.manufacture_id)).distinct
		rdd.toDF("_id", "name", "pack_id", "mole_id", "manufacturer_id").save2Parquet("/test/chc/chcProduct")
	}

	private def savePack(data: RDD[chcData]): Unit = {
		val rdd = data.map(x => (x.pack_id, x.packId, x.pack)).distinct
		rdd.toDF("_id", "packId", "packDesc").save2Parquet("/test/chc/chcPack")
	}

	private def saveOad(data: RDD[chcData]): Unit = {
		val rdd = data.map(x => (x.oad_id, x.atc3, x.oadType)).distinct
		rdd.toDF("_id", "atc3", "oadType").save2Parquet("/test/chc/chcOad")
	}

	private def saveMole(data: RDD[chcData]): Unit = {
		val rdd = data.map(x => (x.mole_id, x.moleName, x.moleNameCN)).distinct
		rdd.toDF("_id", "mole", "moleCN").save2Parquet("/test/chc/chcMole")
	}

	private def savemanufacture(data: RDD[chcData]): Unit = {
		val rdd = data.map(x => (x.manufacture_id, x.manufacture)).distinct
		rdd.toDF("_id", "manufacturer").save2Parquet("/test/chc/chcManufacture")
	}

	def getObjectId(): String = {
		ObjectId.get().toString
	}
}
