package com.pharbers.data.aggregation

import com.pharbers.data.conversion.ProductDevConversion2
import com.pharbers.data.aggregation.functions._
import com.pharbers.data.util.ParquetLocation._
import com.pharbers.data.util._
import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.jobs.sequenceJobWithMap
import com.pharbers.util.log.phLogTrait.phDebugLog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.util.parsing.json.JSONObject

case class CompareMaxResultAgg(args: MapArgs) extends sequenceJobWithMap {



//    val compareFunctions: List[DataFrame => DataFrame] = args("compareFunctions").asInstanceOf[JSONObject]]

//    import scala.reflect.runtime.{universe => ru}
//
//    val classMirror = ru.runtimeMirror(getClass.getClassLoader)         //获取运行时类镜像
//    val classTest = classMirror.staticModule("com.pharbers.data.aggregationJob.functions.GroupFunction")          //获取需要反射object
//    val methods = classMirror.reflectModule(classTest)                  //构造获取方式的对象
//    val objMirror = classMirror.reflect(methods.instance)               //反射结果赋予对象
//    val method = methods.symbol.typeSignature.member(ru.TermName("groupByXYGroup")).asMethod  //反射调用函数
//    val result = objMirror.reflectMethod(method)("", "").asInstanceOf[DataFrame => DataFrame]      //最后带参数,执行这个反射调用的函数

    val sourceId: String = args.get.getOrElse("sourceId", throw new Exception("not found sourceId")).getBy[StringArgs]
    val marketMongo: String = args.get.getOrElse("marketMongo", throw new Exception("not found marketMongo")).getBy[StringArgs]
    val productMongo: String = args.get.getOrElse("productMongo", throw new Exception("not found productMongo")).getBy[StringArgs]

    //todo:配置化
    val productFunctions: List[DataFrame => DataFrame] = List(
        df => df.withColumn("PRODUCT", concat(col("PRODUCT_NAME"), col("PH_CORP_NAME"))),
        aggByGroups(maxResultAggByProductAndYmsExpr, "PRODUCT", "YM", "MARKET"),
        groupByXYGroup("YM", "PRODUCT"),
        ringGRCompare("SALES"),
        yearGRCompare("SALES"),
        groupByXYGroup("YM", "MARKET"),
        somCompare("SALES"),
        rankCompare("SALES"),
        rankCompare("SALES_RING_GROWTH")
    )

    val marketAggFunctions: List[DataFrame => DataFrame] = List(
        aggByGroups(maxResultAggByProductsAndYmsExpr, "MARKET", "YM")
    )

    val salesRankOtherAggFunctions: List[DataFrame => DataFrame] = List(
        distinguishRankTop5AndOther("SALES_RANK", "PRODUCT"),
        aggByGroups(maxResultAggBySalesRankOtherAndYmsExpr, "MARKET", "YM", "SALES_RANK")
    )

    val salesRank10AggFunctions: List[DataFrame => DataFrame] = List(
        findRankTop10("SALES_RANK"),
        aggByGroups(maxResultAggBySalesRankOtherAndYmsExpr, "MARKET", "YM", "SALES_RANK")
    )

    val marketFunctions: List[DataFrame => DataFrame] = List(
        groupByXYGroup("YM", "MARKET"),
        ringGRCompare("SALES"),
        yearGRCompare("SALES"),
        ringGRCompare("PRODUCT_COUNT"),
        yearGRCompare("PRODUCT_COUNT"),
        groupByXYGroup("YM", "COMPANY_ID"),
        somCompare("SALES"),
        groupByXYGroup("YM", "MARKET"),
        ringGRCompare("SALES_SOM"),
        yearGRCompare("SALES_SOM")
    )

    val rankTop10Functions: List[DataFrame => DataFrame] = List(
        groupByXYGroup("YM", "MARKET"),
        somCompare("SALES"),
        filterOne("SALES_RANK", "top10"),
        ringGRCompare("SALES_SOM"),
        yearGRCompare("SALES_SOM")
    )


    override val actions: List[pActionTrait] = List(
        CompareMaxResultAggByFunctions(Map("functions" -> productFunctions, "name" -> "productCompare", "source" -> "maxResultAgg")),
        CompareMaxResultAggByFunctions(Map("functions" -> marketAggFunctions, "name" -> "maxResultMarketAgg", "source" -> "productCompare")),
        CompareMaxResultAggByFunctions(Map("functions" -> marketFunctions, "name" -> "marketCompare", "source" -> "maxResultMarketAgg")),
        CompareMaxResultAggByFunctions(Map("functions" -> salesRankOtherAggFunctions, "name" -> "salesRankOtherAgg", "source" -> "productCompare")),
        CompareMaxResultAggByFunctions(Map("functions" -> salesRank10AggFunctions, "name" -> "salesRank10Agg", "source" -> "productCompare")),
        CompareMaxResultAggByFunctions(Map("functions" -> rankTop10Functions, "name" -> "salesRank10Compare", "source" -> "salesRank10Agg"))
    )
    override val name: String = "CompareMaxResultAggByProduct"

    override def perform(pr: pActionArgs): pActionArgs = {
        val MaxResultAggDF = Parquet2DF(MAX_RESULT_ADDRESS_AGG_LOCATION).filter(col("COMPANY_ID") === sourceId)
        val dfMap = super.perform(MapArgs(Map("maxResultAgg" -> DFArgs(MaxResultAggDF)))).asInstanceOf[MapArgs].get
//        val resultDF = dfMap("salesRank10Compare").get.asInstanceOf[DataFrame]
        dfMap("productCompare").get.asInstanceOf[DataFrame]
                .selectExpr("PRODUCT_NAME","PH_CORP_NAME",  "PRODUCT as MIN_PRODUCT", "YM", "MARKET", "SALES", "COMPANY_ID", "SALES_SOM", "SALES_RANK", "SALES_RING_GROWTH_RANK", "SALES_YEAR_GROWTH", "SALES_RING_GROWTH")
                .withColumn("YM", col("YM").cast(IntegerType))
//                .save2Mongo(productMongo)

        dfMap("marketCompare").get.asInstanceOf[DataFrame]
                .select("MARKET", "YM", "PRODUCT_COUNT", "SALES", "SALES_SOM", "COMPANY_ID", "PRODUCT_COUNT_RING_GROWTH", "PRODUCT_COUNT_YEAR_GROWTH",
                    "SALES_SOM_RING_GROWTH", "SALES_SOM_YEAR_GROWTH")
                .join(
                    dfMap("salesRank10Compare").get.asInstanceOf[DataFrame]
                        .filter(col("SALES_RANK") === "top10")
                        .selectExpr("MARKET as top10MARKET", "YM as topYM", "SALES as CONCENTRATED_SALES",
                            "SALES_SOM as CONCENTRATED_SOM", "SALES_SOM_RING_GROWTH as CONCENTRATED_RING_GROWTH", "SALES_SOM_YEAR_GROWTH as CONCENTRATED_YEAR_GROWTH"),
                    col("top10MARKET") === col("MARKET") && col("topYM") === col("YM"), "left")
                .drop("top10MARKET", "topYM")
                .withColumn("YM", col("YM").cast(IntegerType))
//                .save2Mongo(marketMongo)

        MapArgs(Map(
            "result" -> StringArgs("Conversion success")
        ))
    }
}
