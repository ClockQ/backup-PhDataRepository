package com.pharbers.data.conversion

import com.pharbers.data.model.chcData
import org.apache.spark.sql.DataFrame
import org.bson.types.ObjectId

case class CHCConversion() extends PhDataConversion {

	import org.apache.spark.sql.functions._
	import com.pharbers.data.util.sparkDriver.ss.implicits._

	override def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
		val chcDF = args.getOrElse("chcDF", throw new Exception("not found chcDF"))

		def getObjectId(): String = {
			ObjectId.get().toString
		}

		val rddTemp = chcDF.na.fill("").distinct().toJavaRDD.rdd.map(x => chcData(x(0).toString, x(1).toString,
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


//		midRDD.toDF("_id", "data", "period").show(false)
		println(midRDD.count())
		Map()
	}

	override def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
		val chcBaseDF = args.getOrElse("chcBaseDF", throw new Exception("not found hospBaseDF"))
		val revenueDF = args.getOrElse("revenueDF", Seq.empty[String].toDF("_id"))
		val dateDF = args.getOrElse("dateDF", Seq.empty[String].toDF("_id"))
		val cityDF = args.getOrElse("cityDF", Seq.empty[String].toDF("_id"))
		val productDF = args.getOrElse("productDF", Seq.empty[String].toDF("_id"))
		val packDF = args.getOrElse("packDF", Seq.empty[String].toDF("_id"))
		val oadDF = args.getOrElse("oadDF", Seq.empty[String].toDF("_id"))
		val moleDF = args.getOrElse("moleDF", Seq.empty[String].toDF("_id"))
		val manufactureDF = args.getOrElse("manufactureDF", Seq.empty[String].toDF("_id"))
		val chcDF = chcBaseDF.drop("_id").join(productDF, chcBaseDF("prod_id") === productDF("_id"), "left").drop("_id", "prod_id")
			.join(packDF, col("pack_id") === col("_id"), "left").drop("_id", "pack_id")
			.join(moleDF, col("mole_id") === col("_id"), "left").drop("_id", "mole_id")
			.join(manufactureDF, col("manufacturer_id") === col("_id"), "left").drop("_id", "manufacturer_id")
			.join(oadDF, col("oad_id") === col("_id"), "left").drop("_id", "oad_id")
			.join(revenueDF, col("revenue_id") === col("_id"), "left").drop("_id", "revenue_id")
			.join(dateDF, col("date_id") === col("_id"), "left").drop("_id", "date_id")
			.join(cityDF, col("city_id") === col("_id"), "left").drop("_id", "city_id")
		Map("chcDF" -> chcDF)
	}
}
