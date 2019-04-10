//package com.pharbers
//
//import com.pharbers.common.phFactory
//import com.pharbers.readmongo.phSparkDriver
//import com.pharbers.spark.util.{mongo2RDD, readParquet}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.expressions.UserDefinedFunction
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types._
//
//case class PlayTimeWindowScala(startTime: java.sql.Date, endTime: java.sql.Date)
//
//case class GlobalizedPlayTimeWindowsScala (countries: List[String],
//                                           purchase: List[PlayTimeWindowScala],
//                                           rental: List[PlayTimeWindowScala],
//                                           free: List[PlayTimeWindowScala],
//                                           download: List[PlayTimeWindowScala],
//                                           advertisement: List[PlayTimeWindowScala],
//                                           preorderExclusive: List[PlayTimeWindowScala],
//                                           playTypeIds: Map[String, List[PlayTimeWindowScala]])
//
//class test1 {
//	// corrected schemas:
//	val PlayTimeWindow =
//		StructType(
//			StructField("startTime", DateType, true) ::
//				StructField("endTime", DateType, true) :: Nil)
//
//	val globalizedPlayTimeWindows =
//		StructType(
//			StructField( "countries", ArrayType(StringType, true), true )  ::
//				StructField( "purchase", ArrayType(PlayTimeWindow, true), true )  ::
//				StructField( "rental", ArrayType(PlayTimeWindow, true), true )  ::
//				StructField( "free", ArrayType(PlayTimeWindow, true), true )  ::
//				StructField( "download", ArrayType(PlayTimeWindow, true), true )  ::
//				StructField( "advertisement", ArrayType(PlayTimeWindow, true), true )  ::
//				StructField( "preorderExclusive", ArrayType(PlayTimeWindow, true), true )  ::
//				StructField( "playTypeIds", MapType(StringType, ArrayType(PlayTimeWindow, true), true), true )  ::
//				Nil)
//
//	val schema =    StructType(
//		StructField("id", StringType, true) ::
//			StructField("jazzCount", IntegerType, true) ::
//			StructField("rockCount", IntegerType, true) ::
//			StructField("classicCount", IntegerType, true) ::
//			StructField("nonclassicCount", IntegerType, true) ::
//			StructField("musicType", StringType, true) ::
//			StructField( "playType", globalizedPlayTimeWindows, true) :: Nil)
//
//
//
//	// some conversion methods:
//	def toSqlDate(jDate: java.util.Date): java.sql.Date = new java.sql.Date(jDate.getTime)
//
//	import scala.collection.JavaConverters._
//
//	def toScalaWindowList(l: java.util.List[PlayTimeWindow]): List[PlayTimeWindowScala] = {
//		l.asScala.map(javaWindow => PlayTimeWindowScala(toSqlDate(javaWindow.startTime), toSqlDate(javaWindow.endTime))).toList
//	}
//
//	def toScalaGlobalizedWindows(javaObj: GlobalizedPlayTimeWindows): GlobalizedPlayTimeWindowsScala = {
//		GlobalizedPlayTimeWindowsScala(
//			javaObj.countries.asScala.toList,
//			toScalaWindowList(javaObj.purchase),
//			toScalaWindowList(javaObj.rental),
//			toScalaWindowList(javaObj.free),
//			toScalaWindowList(javaObj.download),
//			toScalaWindowList(javaObj.advertisement),
//			toScalaWindowList(javaObj.preorderExclusive),
//			javaObj.playTypeIds.asScala.mapValues(toScalaWindowList).toMap
//		)
//	}
//
//	val parsedJavaData: RDD[(String, Int, Int, Int, Int, String, GlobalizedPlayTimeWindows)] = mappingFile.map(x => {
//		// your code producing the tuple
//	})
//
//	// convert to Scala objects and into a Row:
//	val inputData = parsedJavaData.map{
//		case (id, jazzCount, rockCount, classicCount, nonclassicCount, musicType, javaPlayType) =>
//			val scalaPlayType = toScalaGlobalizedWindows(javaPlayType)
//			Row(id, jazzCount, rockCount, classicCount, nonclassicCount, musicType, scalaPlayType)
//	}
//
//	// now - this works
//	val inputDataDF = sqlContext.createDataFrame(inputData, schema)
//}
//
//// note the use of java.sql.Date, java.util.Date not supported
//
