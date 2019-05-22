package com.pharbers.data.qitest

import org.apache.spark.sql.DataFrame

/**
  * @description:
  * @author: clock
  * @date: 2019-05-21 22:54
  */
object agg2MaxView extends App {

    import util._
    import com.pharbers.data.util._
    import com.pharbers.data.util.spark._
    import org.apache.spark.sql.functions._
    import sparkDriver.ss.implicits._

    val chcMaxViewParquetPlace = "/test/qi/qi/chcMaxView"

    lazy val aggData = Mongo2DF("aggregateData", "pharbers-aggrate-data")

    lazy val cityDF = OnlineMongo2DF("City")
            .withColumn("TITLE", concat($"TITLE", lit("市")))

    lazy val productDF = OnlineMongo2DF("Product")

    lazy val result = {
        aggData
                .connectMarket() // "INFO_ID"
                .connectDate() // "DATE_TYPE", "DATE"
                .connectAddress(cityDF) // "ADDRESS_TYPE", "ADDRESS_ID"
                .connectProduct(productDF) // "GOODS_TYPE", "GOODS_ID"
                .connectValue() // "VALUE_TYPE", "VALUE"
                .select("INFO_ID", "DATE_TYPE", "DATE", "ADDRESS_TYPE", "ADDRESS_ID", "GOODS_TYPE", "GOODS_ID", "VALUE_TYPE", "VALUE")
                .distinct()
                .generateId
    }

//    OnlineSave2Mongo(result, "SalesRecord")
//    result.save2Parquet(chcMaxViewParquetPlace)

    lazy val chcMaxViewDF = Parquet2DF(chcMaxViewParquetPlace)

    lazy val availableCity = {
        chcMaxViewDF.filter($"ADDRESS_TYPE" =!= 0)
                .select("INFO_ID", "ADDRESS_TYPE", "ADDRESS_ID")
                .distinct()
    }
//    OnlineSave2Mongo(availableCity, "AvailableCity")

    lazy val availableDate = {
        chcMaxViewDF
                .select("INFO_ID", "DATE_TYPE", "DATE")
                .distinct()
    }
//    OnlineSave2Mongo(availableDate, "AvailableDate")



    implicit class AggConversionAction(df: DataFrame) {
        def connectMarket(): DataFrame = {
            df.withColumn("INFO_ID",
                when($"market" === "口服降糖药市场", lit("5ce27089eeefcc0827290c33"))
                        .when($"market" === "钙补充剂市场", lit("5ce36678eeefcc03626e4017"))
            )
        }

        def connectDate(): DataFrame = {
            df
                    .withColumn("DATE_TYPE",
                        when($"date".endsWith("MAT"), 5)
                                .when($"date".endsWith("YTD"), 4)
                                .when(length($"date") === 4, 3)
                                .when($"date".contains("Q"), 2)
                                .when(length($"date") === 6, 1)
                    )
                    .withColumn("DATE", substring($"date", 0, 6))
        }

        def connectAddress(cityDF: DataFrame): DataFrame = {
            df.join(cityDF, $"city" === cityDF("TITLE"), "left")
                    .withColumn("ADDRESS_TYPE",
                        when($"city" === "全国", lit(0))
                                .when(cityDF("_id").isNotNull, lit(1))
                    )
                    .withColumn("ADDRESS_ID",
                        when($"city" === "全国", lit("total"))
                                .when(cityDF("_id").isNotNull, cityDF("_id")))
                    .drop(cityDF("_id"))
        }

        def connectProduct(productDF: DataFrame): DataFrame = {
            val classifyDF = df
                    .withColumn("GOODS_TYPE",
                        when($"keyType" === "city", lit(0))
                                .when($"key" === "total", lit(0))
                                .when($"keyType" === "prod", lit(1))
                                .when($"keyType" === "mole", lit(2))
                                .when($"keyType" === "corp", lit(3))
                                .when($"keyType" === "oad", lit(5))
                    )

            val totalClassDF = classifyDF.filter($"GOODS_TYPE" === 0)
                    .withColumn("GOODS_ID", lit("total"))

            val prodDF = productDF.withColumn("pc", concat(productDF("TITLE"), lit(31.toChar.toString), productDF("CORP_NAME"))).select("_id", "pc")
            val prodClassDF = classifyDF.filter($"GOODS_TYPE" === 1)
                    .join(prodDF
                        , df("key") === prodDF("pc")
                        , "left"
                    )
                    .withColumn("GOODS_ID", prodDF("_id"))
                    .drop(prodDF("_id"))
                    .drop(prodDF("pc"))

            val otherClassDF = classifyDF.filter($"GOODS_TYPE" =!= 0 && $"GOODS_TYPE" =!= 1)
                    .withColumn("GOODS_ID", $"key")

            totalClassDF unionByName prodClassDF unionByName otherClassDF
        }

        def connectValue(): DataFrame = {
            df
                    .withColumn("VALUE_TYPE",
                        when($"valueType" === "sales", lit(1))
                                .when($"valueType" === "growth", lit(2))
                                .when($"valueType" === "share", lit(3))
                                .when($"valueType" === "shareGrowth", lit(4))
                                .when($"valueType" === "EI", lit(5))
                                .when($"valueType" === "moleShare", lit(6))
                                .when($"valueType" === "moleShareGrowth", lit(7))
                                .when($"valueType" === "moleEI", lit(8))
                    )
                    .withColumn("VALUE", $"value")
        }
    }

// 产品转换
//    lazy val productDF = {
//        val devProductDF = Mongo2DF("prod_dev9")
//        devProductDF.withColumnRenamed("_id", "_id")
//                .withColumnRenamed("DEV_PRODUCT_NAME", "TITLE")
//                .withColumnRenamed("DEV_CORP_NAME", "CORP_NAME")
//                .withColumnRenamed("DEV_MOLE_NAME", "MOLE_NAME")
//                .withColumnRenamed("DEV_PACKAGE_DES", "PACKAGE_DES")
//                .withColumnRenamed("DEV_PACKAGE_NUMBER", "PACKAGE_NUMBER")
//                .withColumnRenamed("DEV_DOSAGE_NAME", "DOSAGE_NAME")
//                .withColumn("DELIVERY_WAY", lit(""))
//                .distinctByKey("TITLE", "CORP_NAME")()
//    }
//    OnlineSave2Mongo(productDF, "Product")

}
