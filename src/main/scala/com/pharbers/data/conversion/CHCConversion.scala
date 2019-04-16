package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

case class CHCConversion() extends PhDataConversion {

	import com.pharbers.data.util.sparkDriver.ss.implicits._
	import org.apache.spark.sql.functions._

	override def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = ???

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
