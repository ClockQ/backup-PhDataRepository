package com.pharbers.run

import com.pharbers.data.conversion
import com.pharbers.util.log.phLogTrait.phDebugLog

object TransformMarket extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._

    val pfizer_source_id = "5ca069e2eeefcc012918ec73"
    val pfizer_AI_D_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-AI_D.csv"
    val pfizer_AI_R_other_csv = "/workData/Export/88339191-a312-d9e3-6d8e-b7443f434aea/5b028f95ed925c2c705b85ba-201804-AI_R_other.csv"

    val prodDevDIS = Parquet2DF(PROD_DEV_LOCATION)
            .withColumn("MIN2",
                concat(col("PRODUCT_NAME"), col("DOSAGE_NAME"), col("PACKAGE_DES"), col("PACKAGE_NUMBER"), col("CORP_NAME"))
            )

    val pfizerAI_DMaxResultDF = FILE2DF(pfizer_AI_D_csv, 31.toChar.toString)
    val AI_D_MARKET = pfizerAI_DMaxResultDF
            .join(
                prodDevDIS, pfizerAI_DMaxResultDF("Product") === prodDevDIS("MIN2")
            )
            .select("_id", "MARKET")
            .distinct()

    val pfizerAI_R_otherMaxResultDF = FILE2DF(pfizer_AI_R_other_csv, 31.toChar.toString)
    val AI_R_other_MARKET = pfizerAI_R_otherMaxResultDF
            .join(
                prodDevDIS, pfizerAI_R_otherMaxResultDF("Product") === prodDevDIS("MIN2")
            )
            .select("_id", "MARKET")
            .distinct()

    println(AI_D_MARKET.count()) //160
    println(AI_R_other_MARKET.count()) // 2246
    println(AI_D_MARKET.union(AI_R_other_MARKET).count()) // 2406
    println(AI_D_MARKET.union(AI_R_other_MARKET).dropDuplicates("_id").count()) // 2406

}
