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
        val rddTemp = data.toJavaRDD.rdd.map(x => addressData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString,
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
        })
        getTier(refData, "test")
        getPrefecture(refData, getPolygon())
    }

    //todo:objectID
    private def getObjectID(): String ={
        "null"
    }

    private def getPolygon(): String ={
        "null"
    }

    private def getTier(data: RDD[addressData], tag: String): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._
//        data.foreach(x => println(x))
        val df = data.map(x => {
            tierData(x.tierID, x.tier, tag)
        }).distinct.toDF("_id", "Tier", "tag")
        saveParquet(df, "/test/testAddress/", "tier")
    }

    private def getPrefecture(data: RDD[addressData], polygon: String): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val df = data.map(x => {
            prefectureData(x.prefectureID, x.prefecture, polygon, x.cityID)
        }).distinct.toDF("_id", "name", "polygon", "city")
        saveParquet(df, "/test/testAddress/", "prefecture")
    }

    private def getCity(data: RDD[addressData], polygon: String): Unit ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val df = data.map(x => {
            prefectureData(x.prefectureID, x.prefecture, polygon, x.cityID)
        }).distinct.toDF("_id", "name", "polygon", "city")
        saveParquet(df, "/test/testAddress/", "prefecture")
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
