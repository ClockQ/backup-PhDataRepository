package com.pharbers.data.aggregation

import com.pharbers.data.aggregation.RDDFunction.MaxResultAggRDDFunc
import com.pharbers.data.util.{CSV2DF, Parquet2DF}
import com.pharbers.pactions.actionbase._
import com.pharbers.pactions.jobs.sequenceJobWithMap
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, sum}
import org.apache.spark.sql.types.IntegerType
import com.pharbers.data.util._

case class CompareMaxResultCityAgg(args: MapArgs) extends sequenceJobWithMap {
    override val actions: List[pActionTrait] = Nil
    override val name: String = "maxResultAgg"

    val sourceId: String = args.get.getOrElse("sourceId", throw new Exception("not found sourceId")).getBy[StringArgs]
    val marketMongo: String = args.get.getOrElse("marketMongo", throw new Exception("not found marketMongo")).getBy[StringArgs]
    val productMongo: String = args.get.getOrElse("productMongo", throw new Exception("not found productMongo")).getBy[StringArgs]
    val maxAgg: String = args.get.getOrElse("maxAgg", throw new Exception("not found productMongo")).getBy[StringArgs]
    val market: String = args.get.getOrElse("market", throw new Exception("not found productMongo")).getBy[StringArgs]
    val ymType: String = args.get.getOrElse("ymType", throw new Exception("not found productMongo")).getBy[StringArgs]
    val addressType: String = args.get.getOrElse("addressType", throw new Exception("not found productMongo")).getBy[StringArgs]

    override def perform(pr: pActionArgs): pActionArgs = {
        var MaxResultAggDF = Parquet2DF(maxAgg)
                .filter(col("COMPANY_ID") === sourceId && col("PRODUCT_NAME") =!= "" && col("CITY") =!= "")

        val marketDF = CSV2DF(market)

        val windowMAT = Window.partitionBy("MIN_PRODUCT", addressType).orderBy(col("YM").cast(IntegerType)).rangeBetween(-100, 0)

        ymType match {
            case "MAT" => MaxResultAggDF = MaxResultAggDF.withColumn("SALES", sum(col("SALES")).over(windowMAT))
            case "YTD" => MaxResultAggDF = MaxResultAggRDDFunc.addYTDSales(MaxResultAggDF)
        }

        addressType match {
            case "NATIONAL" => MaxResultAggDF = MaxResultAggDF.withColumn("ADDRESS", lit(addressType)).withColumn("TIER", lit(0))
            case "CITY" => MaxResultAggDF = MaxResultAggDF.withColumn("ADDRESS", col(addressType))
            case _ => MaxResultAggDF = MaxResultAggDF.withColumn("ADDRESS", col(addressType)).withColumn("TIER", lit(0))
        }

        MaxResultAggDF =
                MaxResultAggDF
                        .withColumn("YM_TYPE", lit(ymType))
                        .withColumn("ADDRESS_TYPE", lit(addressType))
                        .join(
                            marketDF.withColumnRenamed("MOLE_NAME", "mole"),
                            col("MOLE_NAME") === col("mole"),
                            "left")
                        .drop("mole")

        DFArgs(MaxResultAggDF)
    }
}
