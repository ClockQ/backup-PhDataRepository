package com.pharbers.data.aggregation.RDDFunction

import org.apache.spark.sql.DataFrame
import com.pharbers.data.util._

object MaxResultAggRDDFunc {
    val columns = List(
        "MIN_PRODUCT",
        "YM", "CITY",
        "REGION", "PROVINCE","TIER",
        "SALES", "UNITS",
        "COMPANY_ID", "PRODUCT_NAME",
        "MOLE_NAME", "CORP_NAME",
        "PH_CORP_NAME", "time"
    )
    def addYTDSales(df: DataFrame): DataFrame ={
        import sparkDriver.ss.implicits._

        val rdd = df.rdd.map(x => {
            columns.zipWithIndex.map{case (_, index) => x(index).toString}
        })
        rdd
                .map(x => {
                    val salesList: List[Double] = (
                            List.fill(13 - x(columns.indexOf("YM")).toInt % 100)(x(columns.indexOf("SALES")).toDouble)
                                    ::: List.fill(12)(0.0)
                            ).take(12)
                    (x(columns.indexOf("MIN_PRODUCT")) + x(columns.indexOf("CITY")) + x(columns.indexOf("YM")).toInt / 100, x, salesList)
                })
                .groupBy(x => x._1)
                .map(x => {
                    x._2.reduce((left, right) => {
                        val leftList = left._3
                        val rightList = right._3
                        (left._1, left._2, leftList.zip(rightList).map(x => x._1 + x._2))
                    })
                })
                .flatMap(x => {
                    x._3.zipWithIndex.map(sales => {
                        columns.map {
                            case "YM" => (x._2(columns.indexOf("YM")).toInt / 100 * 100 + (12 - sales._2)).toString
                            case "SALES" => sales._1.toString
                            case col => x._2(columns.indexOf(col))
                        }
                    })
                })
                .map(x => (x.head, x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12), x(13)))
                .toDF(
                    "MIN_PRODUCT",
                    "YM", "CITY",
                    "REGION", "PROVINCE","TIER",
                    "SALES", "UNITS",
                    "COMPANY_ID", "PRODUCT_NAME",
                    "MOLE_NAME", "CORP_NAME",
                    "PH_CORP_NAME", "time"
                )
    }
}

