package phDataConversion

import com.pharbers.spark.phSparkDriver
import model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import com.pharbers.spark.util.dataFrame2Mongo
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import common.phFactory

class phRegionData extends Serializable {
    def getRegionDataFromCsv(df: DataFrame): Unit ={

        val data = df.select("Region", "location", "Province", "City", "Prefecture", "City Tier 2010").na.fill("")
        val rddTemp = data.toJavaRDD.rdd.map(x => addressExcelData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
            x(5).toString.trim.toInt))

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
        }).groupBy(x => x.city).flatMap(x => {
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
        getTier(refData, "test")
        getPrefecture(refData, getPolygon())
        getCity(refData, getPolygon())
        getProvince(refData, getPolygon())
        getRegion(refData, "test")
        getAddress(refData)
    }

    //todo:objectID
    private def getObjectID(): String ={
        "null"
    }

    private def getPolygon(): String ={
        "null"
    }

    private def getTier(data: RDD[addressExcelData], tag: String): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._
        val df = data.map(x => {
            tierData(x.tierID, x.tier, tag)
        }).distinct.toDF("_id", "Tier", "tag")
        saveParquet(df, "/test/testAddress/", "tier")
    }

    private def getPrefecture(data: RDD[addressExcelData], polygon: String): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val df = data.map(x => {
            prefectureData(x.prefectureID, x.prefecture, polygon, x.cityID)
        }).distinct.toDF("_id", "name", "polygon", "city")
        saveParquet(df, "/test/testAddress/", "prefecture")
    }

    private def getCity(data: RDD[addressExcelData], polygon: String): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val df = data.map(x => {
            cityData(x.cityID, x.city, polygon, List(x.tierID), x.provinceID)
        }).distinct.toDF("_id", "name", "polygon", "tier", "province")
        saveParquet(df, "/test/testAddress/", "city")
    }

    private def getProvince(data: RDD[addressExcelData], polygon: String): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val df = data.map(x => {
            provinceData(x.provinceID, x.province, polygon)
        }).distinct.toDF("_id", "name", "polygon")
        saveParquet(df, "/test/testAddress/", "province")
    }

    private def getRegion(data: RDD[addressExcelData], tag: String): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._
        val df = data.map(x => {
            regionData(x.regionID, x.region, tag)
        }).distinct.toDF("_id", "name", "tag")
        saveParquet(df, "/test/testAddress/", "region")
    }

    private def getAddress(data: RDD[addressExcelData]): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._
        val df = data.map(x => {
            addressData(x.addressID, pointPolygon(x.location.split(",")).toString, List(x.regionID))
        }).distinct.toDF("_id", "location", "region", "desc")
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
                println ("error: " + ex)
            }
        }
    }
}
