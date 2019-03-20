package com.pharbers.phDataConversion

import com.pharbers.spark.phSparkDriver
import com.pharbers.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions._
import com.pharbers.spark.util.{dataFrame2Mongo, readParquet}
import com.pharbers.common.phFactory
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.bson.types.ObjectId

class phRegionData extends Serializable {
	def getRegionDataFromCsv(df: DataFrame): Unit = {

		val data = df.select("Region", "location", "Province", "City", "Prefecture", "City Tier 2010").na.fill("")
		val rddTemp = data.toJavaRDD.rdd.map(x => addressExcelData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
			x(5).toString.trim))

		//        rddTemp.foreach(print)
		val refData = rddTemp.map(x => {
			x.addressID = getObjectID()
			x
		}).groupBy(x => x.prefecture).flatMap(x => {
			val prefectureID = getObjectID()
			x._2.map(y => {
				y.prefectureID = prefectureID
				y
			})
		}).groupBy(x => x.city + x.province).flatMap(x => {
			val cityID = getObjectID()
			x._2.map(y => {
				y.cityID = cityID
				y
			})
		}).groupBy(x => x.province).flatMap(x => {
			val provinceID = getObjectID()
			x._2.map(y => {
				y.provinceID = provinceID
				y
			})
		}).groupBy(x => x.tier).flatMap(x => {
			val tierID = getObjectID()
			x._2.map(y => {
				y.tierID = tierID
				y
			})
		}).groupBy(x => x.region).flatMap(x => {
			val regionID = getObjectID()
			x._2.map(y => {
				y.regionID = regionID
				y
			})
		})

		getTier(refData, "2010")
		getPrefecture(refData, getPolygon())
		getCity(refData, getPolygon())
		getProvince(refData, getPolygon())
		getRegion(refData, "test")
		getAddress(refData)
	}

	def add18Tiger(cityDF: DataFrame, cityTier2010DF: DataFrame, cityTierDf: DataFrame): Unit = {
		val driver = phFactory.getSparkInstance()
		import driver.ss.implicits._
		//udf函数
		val setIdCol: UserDefinedFunction = udf{
			getObjectID()
		}
		val settier: UserDefinedFunction = udf{
			(seq: Seq[String], str: String) => seq :+ str
		}

		val setTier: (String, String) => DataFrame = (cityTier, tagStr) => {
			cityTierDf.select("Prefecture", cityTier)
				.distinct()
				.withColumnRenamed(cityTier, "tier")
    			.withColumnRenamed("Prefecture", "city")
				.withColumn("tag", lit(tagStr))
				.withColumn("_id", setIdCol())
		}
		val CT2018DFWithCity = setTier("City Tier 2018", "2018")
		val cityTier2018DF = CT2018DFWithCity.select("_id", "tier", "tag")
		val allTier = cityTier2018DF.union(cityTier2010DF)
		saveParquet(allTier, "/test/testAddress/", "tier")
		val renameCT2018 = CT2018DFWithCity.withColumnRenamed("tier", "tier_c")
    		.withColumnRenamed("_id", "_id_c")
		val joinedDF = cityDF.select("_id", "name", "polygon", "tier", "province")
    		.join(renameCT2018, col("name") === col("city"), "left")
    		.na.fill("")
		val haveNoCityTier2018 = joinedDF.filter(col("tier") === "")
		val haveCityTier2018 = joinedDF.filter(col("tier") =!= "")
    		.withColumn("tier", settier(col("tier"), col("_id_c")))
		val resultDF = haveNoCityTier2018.union(haveCityTier2018).select("_id", "name", "polygon", "tier", "province")
		saveParquet(resultDF, "/test/testAddress/", "city")
	}


	private def getObjectID(): String = {
		ObjectId.get().toString
	}

	private def getPolygon(): String = {
		"null"
	}

	private def getTier(data: RDD[addressExcelData], tag: String): Unit = {
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._
		val df = data.map(x => {
			tierData(x.tierID, x.tier, tag)
		}).distinct

		saveParquet(df.toDF("_id", "Tier", "tag"), "/test/testAddress/", "tier")
	}

	private def getPrefecture(data: RDD[addressExcelData], polygon: String): Unit = {
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val df = data.map(x => {
			prefectureData(x.prefectureID, x.prefecture, polygon, x.cityID)
		}).distinct.toDF("_id", "name", "polygon", "city")
		saveParquet(df, "/test/testAddress/", "prefecture")
	}

	private def getCity(data: RDD[addressExcelData], polygon: String): Unit = {
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val df = data.map(x => {
			cityData(x.cityID, x.city, polygon, List(x.tierID), x.provinceID)
		}).groupBy(x => x.name).map(x => {
			x._2.reduce((left, right) => {
				left.tier = left.tier ::: right.tier
				left
			})
		}).distinct.toDF("_id", "name", "polygon", "tier", "province")
		saveParquet(df, "/test/testAddress/", "city")
	}

	private def getProvince(data: RDD[addressExcelData], polygon: String): Unit = {
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val df = data.map(x => {
			provinceData(x.provinceID, x.province, polygon)
		}).distinct.toDF("_id", "name", "polygon")
		saveParquet(df, "/test/testAddress/", "province")
	}

	private def getRegion(data: RDD[addressExcelData], tag: String): Unit = {
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._
		val df = data.map(x => {
			regionData(x.regionID, x.region, tag)
		}).distinct.toDF("_id", "name", "tag")
		saveParquet(df, "/test/testAddress/", "region")
	}

	private def getAddress(data: RDD[addressExcelData]): Unit = {
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._
		val df = data.map(x => {
			addressData(x.addressID, pointPolygon(x.location.split(",")), x.prefectureID, List(x.regionID))
		}).distinct.toDF("_id", "location", "prefecture", "region", "desc")
		saveParquet(df, "/test/testAddress/", "address")
	}


	private def saveParquet(df: DataFrame, path: String, name: String): Unit = {

		try {
			df.write.mode(SaveMode.Append)
				.option("header", value = true)
				.parquet(path + name)
		} catch {
			case ex: org.apache.spark.sql.AnalysisException => {
				println("error: " + ex)
			}
			case ex: java.lang.ArrayIndexOutOfBoundsException => {
				println("error: " + ex)
			}
		}
	}
}
