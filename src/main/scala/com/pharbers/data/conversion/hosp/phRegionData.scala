package com.pharbers.data.conversion.hosp

import com.pharbers.data.conversion.hosp.model._
import com.pharbers.spark.phSparkDriver
import com.pharbers.spark.util.readParquet
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.bson.types.ObjectId


class phRegionData extends Serializable {
	def getRegionDataFromCsv(df: DataFrame): Unit = {

		val data = df.select("Region", "location", "Province", "City", "Prefecture", "City Tier 2010", "addressId")
				.na.fill("")
		val rddTemp = data.toJavaRDD.rdd.map(x => addressExcelData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
			x(5).toString, x(6).toString))

        val refData = rddTemp.map(x => {
			x.desc = x.province + x.city + x.prefecture
            x
        }).groupBy(x => x.prefecture + x.province + x.city).flatMap(x => {
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
        }).cache()
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val medleDf = refData.toDF("region", "location", "province", "city", "prefecture", "tier",
            "addressID", "prefectureID", "cityID", "provinceID", "tierID", "regionID", "desc")

//        saveParquet(medleDf ,"/repository/", "medle")

//        val medleRDD = getRefData()
        getPrefecture(refData, getPolygon())
        getCity(refData, getPolygon())
        getProvince(refData, getPolygon())
        getRegion(refData, "201903Pharbers")
        getAddress(refData)
    }

	def add18Tiger(cityDF: DataFrame, cityTier2010DF: DataFrame, cityTierDf: DataFrame)(setIdCol: UserDefinedFunction): Unit = {
		val settier: UserDefinedFunction = udf{
			(seq: Seq[String], str: String) => seq :+ str
		}
		val cityTierWithIdReload = cityTierDf.select("City Tier 2018").distinct()
				.withColumn("_id", setIdCol())
        		.withColumn("tag", lit("2018"))
        		.select("_id", "City Tier 2018", "tag")
		cityTierWithIdReload.cache()
		val setTier: String => DataFrame = cityTier => {
			val resultWithoutId = cityTierDf.select("Prefecture", cityTier)
				.distinct()
				.withColumnRenamed(cityTier, "tier")
    			.withColumnRenamed("Prefecture", "city")
			val result = resultWithoutId.join(cityTierWithIdReload, col("tier") === col(cityTier))
        			.select("city","_id", "tier", "tag")
			result
		}
		val CT2018DFWithCity = setTier("City Tier 2018")
		val allTier = cityTierWithIdReload.withColumnRenamed("City Tier 2018", "tier").union(cityTier2010DF)
		saveParquet(allTier, "/repository/", "tier")
		val renameCT2018 = CT2018DFWithCity.withColumnRenamed("tier", "tier_c")
    		.withColumnRenamed("_id", "_id_c")
		val joinedDF = cityDF.select("_id", "name", "polygon", "tier", "province")
    		.join(renameCT2018, col("name") === col("city"), "left")
    		.na.fill("")
		val haveNoCityTier2018 = joinedDF.filter(col("tier_c") === "")
		val haveCityTier2018 = joinedDF.filter(col("tier_c") =!= "")
    		.withColumn("tier", settier(col("tier"), col("_id_c")))
		val resultDF = haveNoCityTier2018.union(haveCityTier2018).select("_id", "name", "polygon", "tier", "province")
		saveParquet(resultDF, "/repository/", "city")
	}

    def getRefData(): RDD[addressExcelData] ={
        val driver = phFactory.getSparkInstance()
        import driver.conn_instance

        driver.setUtil(readParquet()).readParquet("/repository/medle")
                .toJavaRDD.rdd.map(x => addressExcelData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
                x(5).toString, x(6).toString,x(7).toString,x(8).toString,x(9).toString,x(10).toString,x(11).toString, x(12).toString))
    }

	private def getObjectID(): String ={
        ObjectId.get().toString
    }

	private def getPolygon(): polygon = {
		polygon(Nil)
	}


	private def getPrefecture(data: RDD[addressExcelData], polygon: polygon): Unit = {
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val df = data.map(x => {
			prefectureData(x.prefectureID, x.prefecture, polygon, x.cityID)
		}).distinct.toDF("_id", "name", "polygon", "city")
		saveParquet(df, "/repository/", "prefecture")
	}

	private def getCity(data: RDD[addressExcelData], polygon: polygon): Unit = {
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

        val tierDf = data.map(x => {
            tierData(x.tierID, x.tier, "2010")
        }).distinct.toDF("_id", "Tier", "tag")

		val cityDf = data.map(x => {
			cityData(x.cityID, x.city, polygon, List(x.tierID), x.provinceID)
		}).groupBy(x => x.name).map(x => {
			x._2.reduce((left, right) => {
				left.tier = (left.tier ::: right.tier).distinct
				left
			})
		}).distinct.toDF("_id", "name", "polygon", "tier", "province")

        val tier18Df = sparkDriver.ss.read.format("com.databricks.spark.csv")
                .option("header", "true")
                .option("delimiter", ",")
                .load("/test/10and18tier.csv")

        add18Tiger(cityDf, tierDf, tier18Df)(phDataHandFunc.setIdCol)
	}

	private def getProvince(data: RDD[addressExcelData], polygon: polygon): Unit = {
		lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
		import sparkDriver.ss.implicits._

		val df = data.map(x => {
			provinceData(x.provinceID, x.province, polygon)
		}).distinct.toDF("_id", "name", "polygon")
		saveParquet(df, "/repository/", "province")
	}

    private def getRegion(data: RDD[addressExcelData], tag: String): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._
        val df = data.map(x => {
            regionData(x.regionID, x.region, tag)
        }).distinct
        saveParquet(df.toDF("_id", "name", "tag"), "/repository/", "region")
    }

    private def getAddress(data: RDD[addressExcelData]): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val df = data.map(x => {
            addressData(x.addressID, pointPolygon(x.location.split(",")), x.prefectureID, List(x.regionID), x.desc)
        }).distinct
        saveParquet(df.toDF("_id", "location", "prefecture", "region", "desc"), "/repository/", "address")
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
