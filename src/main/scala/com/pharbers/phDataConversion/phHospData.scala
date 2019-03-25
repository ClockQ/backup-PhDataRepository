package com.pharbers.phDataConversion

import com.pharbers.spark.phSparkDriver
import com.pharbers.model.hosp._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions._
import com.pharbers.spark.util.{dataFrame2Mongo, readParquet}
import com.pharbers.common.phFactory
import org.apache.spark.sql.expressions.UserDefinedFunction


class phHospData() extends Serializable{
    def getHospDataFromCsv(df: DataFrame): Unit ={

        val data = df.withColumn("hospId", phDataHandFunc.setIdCol())
                .na.fill("")
                .cache()

        val hosp = List("hospId", "新版名称", "Type", "Hosp_level","性质", "addressId")
        val numMap = Map("outpatient" -> List("年诊疗人次", "内科诊次", "内科诊次", "外科诊次", "入院人数", "住院病人手术人次数"),
            "bed" -> List("床位数", "全科床位数", "内科床位数", "外科床位数", "眼科床位数"),
            "revenue" -> List("医疗收入", "门诊收入", "门诊治疗收入", "门诊手术收入", "住院收入", "住院床位收入", "住院治疗收入", "住院手术收入", "药品收入", "门诊药品收入", "门诊西药收入", "住院药品收入", "住院西药收入"),
            "staff" -> List("医生数", "在职员工人数"))

        val hospRDD = getEstimate(data, "2019")

        numMap.foreach{case (name, list) =>
            hospRDD union getNumbers(getRdd(hosp, numMap(name), data), "2019", name, list)(hospSetNumberId(name), numbersToDf(name))
        }
        hospRDD.keyBy(x => x._id).reduce((left, right) => {
            left._2.revenues = (right._2.revenues ::: right._2.revenues).distinct
            left._2.nobs = (right._2.nobs ::: right._2.nobs).distinct
            left._2.estimates = (right._2.estimates ::: right._2.estimates).distinct
            left._2.noo = (right._2.noo ::: right._2.noo).distinct
            left._2.nos = (right._2.nos ::: right._2.nos).distinct
            left
        })

    }

    def getRdd(hosp: List[String], numbers: List[String], df: DataFrame): RDD[data] ={
        df.select("PHAHospname", hosp ::: numbers:_*).javaRDD.rdd.map(x =>

            data(hospData(x(1).toString, x(2).toString, x(3).toString, x(4).toString, x(5).toString, x(6).toString),
                collection.mutable.ArrayBuffer.apply(numbers.map(y => (x(numbers.indexOf(y) + 7).toString, "", "")):_*)))
    }


    def getEstimate(df: DataFrame, tag: String): RDD[hospData] = {
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._

        val res = df.select("hospId", "新版名称", "Type", "Hosp_level","性质", "addressId", "Est_DrugIncome_RMB")
                .javaRDD.rdd.map(x => (hospData(x(0).toString, x(1).toString, x(2).toString, x(3).toString, x(4).toString, x(5).toString), x(6).toString))
                .groupBy(x => x._2)
                .flatMap(x => {
                    val id = phDataHandFunc.getObjectID()
                    x._2.map(y => {
                        y._1.estimates = List(id)
                        (y._1, y._2, id)
                    })
                }).cache()

        phDataHandFunc.saveParquet(res.map(x => (x._3,"Est_DrugIncome_RMB", "tag", x._2.trim.toInt)).distinct().toDF("_id", "title", "tag", "amount"),
            "/test/hosp/","estimate")

        res.map(x => x._1)
    }

    def getNumbers(rdd: RDD[data], tag: String, name: String, titleList: List[String])
                  (func: (String ,hospData) => hospData, toDfFunc: RDD[data]=> String => DataFrame ): RDD[hospData] = {

        var rddData = rdd
        titleList.zipWithIndex.foreach { case (title, index) =>
            rddData = rddData.groupBy(x => x.array(index)._1)
                    .flatMap(x => {
                        val id = phDataHandFunc.getObjectID()
                        x._2.map(y => {
                            y.array(index) = (y.array(index)._1, title, id)
                            y.hosp.noo = (y.hosp.noo :+ id).distinct
                            y
                        })
                    }).cache()
        }

        phDataHandFunc.saveParquet(toDfFunc(rddData)(tag),"/test/hosp/", name)
        rddData.map(x => x.hosp)
    }

    def numbersToDf(name: String, rmbId: String = "")(rddData: RDD[data])(tag: String): DataFrame ={
        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
        import sparkDriver.ss.implicits._
        Map("revenue" -> rddData.map(x => x.array).flatMap(x => x.map(y => (y._1, y._2, y._3, y._2 + tag, 365, rmbId))).toDF("amount", "title", "_id", "tag", "period", "unit")
        ).getOrElse(name, rddData.map(x => x.array).flatMap(x => x.map(y => (y._1, y._2, y._3, y._2 + tag))).toDF("amount", "title", "_id", "tag"))

    }

    def hospSetNumberId(name: String)(id: String, hosp: hospData): hospData = {
        Map("outpatient" -> { hosp.noo = (hosp.noo :+ id).distinct; hosp},
            "bed" -> { hosp.nobs = (hosp.nobs :+ id).distinct; hosp},
            "revenue" -> { hosp.revenues = (hosp.revenues :+ id).distinct; hosp},
            "staff" -> { hosp.nos = (hosp.nos :+ id).distinct; hosp},
        )(name)
    }

//    def revenueToDf(rddData: RDD[(hospData, List[(String, String, String)])])(rmbId: String)(tag: String): DataFrame ={
//        lazy val sparkDriver: phSparkDriver = phFactory.getSparkInstance()
//        import sparkDriver.ss.implicits._
//
//        rddData.map(x => x._2).flatMap(x => x.map(y => (y._1, y._2, y._3, y._2 + tag, 365, rmbId))).toDF("amount", "title", "_id", "tag", "period", "unit")
//    }
}

case class data(hosp: hospData, array: collection.mutable.ArrayBuffer[(String, String, String)])