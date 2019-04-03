package com.pharbers.data.conversion

import org.apache.spark.sql.DataFrame

case class CHCConversion() extends PhDataConversion {

	import com.pharbers.data.util._
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
		val chcDF = chcBaseDF.drop("_id").join(revenueDF, col("revenue") === col("_id"), "left").drop("_id")
			.join(dateDF, col("date") === col("_id"), "left").drop("_id").drop("date")
    		.withColumnRenamed("title", "date")
			.join(cityDF, col("city") === col("_id"), "left").drop("_id").drop("city")
    		.withColumnRenamed("title", "city")
			.join(productDF, col("prod") === col("_id"), "left").drop("_id").drop("prod")
    		.withColumnRenamed("name", "product")
			.join(packDF, col("pack") === col("_id"), "left").drop("_id").drop("pack")
    		.withColumnRenamed("oad", "oad_p")
			.join(oadDF, col("oad_p") === col("_id"), "left").drop("_id").drop("oad_p")
			.join(moleDF, col("mole") === col("_id"), "left").drop("_id").drop("mole")
			.join(manufactureDF, col("manufacture") === col("_id"), "left").drop("_id")
			.withColumnRenamed("title", "manufacture")
		Map("chcDF" -> chcDF)
	}
}
