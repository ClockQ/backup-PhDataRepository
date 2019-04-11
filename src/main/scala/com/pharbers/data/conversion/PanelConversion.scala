package com.pharbers.data.conversion
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.pharbers.data.util.commonUDF.generateIdUdf
import com.pharbers.data.util._
import com.pharbers.util.log.phLogTrait.phDebugLog


case class PanelConversion(company_id: String)
        extends PhDataConversion {


    override def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val panelDF = args.getOrElse("panelDF", throw new Exception("not found panelDF"))
        val hospDF = args.getOrElse("hospDF", throw new Exception("not found hospDF"))
        val sourceDF = args.getOrElse("sourceDF", throw new Exception("not found sourceDF"))

        val connHospSource = panelDF.withColumnRenamed("HOSP_ID", "PHA_ID")
                .withColumn("source_id", lit(company_id))
                .join(hospDF.withColumnRenamed("_id", "HOSP_ID"), panelDF("PHA_ID") === hospDF("PHAHospId"), "left")
                .join(sourceDF.withColumnRenamed("_id", "SOURCE_ID"),
                    panelDF("source_id") === sourceDF("COMPANY_ID") && panelDF("DOI") === sourceDF("MARKET"), "left")

        val notConnHospOfPanel = connHospSource.filter(col("HOSP_ID").isNull)
        val notConnHospOfPanelCount = notConnHospOfPanel.count()
        if (notConnHospOfPanelCount != 0) {
            phDebugLog(notConnHospOfPanelCount + "条医院未匹配, 重新转换")
            val notConnHospDIS = notConnHospOfPanel.select("PHA_ID", "Hosp_name")
                    .distinct()
                    .withColumn("HOSP_ID", generateIdUdf())
                    .withColumnRenamed("PHA_ID", "PHAHospId")
                    .withColumnRenamed("Hosp_name", "title")
                    .cache()

            return toERD(args +
                    ("hospDF" -> hospDF.unionByName(notConnHospDIS.alignAt(hospDF)))
            )
        }

        val notConnSourceOfPanel = connHospSource.filter(col("SOURCE_ID").isNull)
        val notConnSourceOfPanelCount = notConnHospOfPanel.count()
        if (notConnSourceOfPanelCount != 0) {
            phDebugLog(notConnSourceOfPanelCount + "条source未匹配, 重新转换")
            val notConnSourceDIS = notConnSourceOfPanel.select("source_id", "DOI")
                    .distinct()
                    .withColumn("SOURCE_ID", generateIdUdf())
                    .withColumnRenamed("source_id", "COMPANY_ID")
                    .withColumnRenamed("DOI", "MARKET")
                    .cache()

            return toERD(args +
                    ("sourceDF" -> hospDF.unionByName(notConnSourceDIS.alignAt(hospDF)))
            )
        }

        val panelERD = connHospSource
                .generateId
                .str2Time
                .trim("PRODUCT_NAME_NOTE")
                .select("_id", "SOURCE_ID", "Date", "HOSP_ID", "Prod_Name", "Sales", "Units")
                .withColumnRenamed("Date", "DATE")
                .withColumnRenamed("Prod_Name", "MIN_PRODUCT")
                .withColumnRenamed("Sales", "SALE")
                .withColumnRenamed("Units", "UNIT")
        Map(
            "panelERD" -> panelERD,
            "hospDIS" -> hospDF,
            "sourceDIS" -> sourceDF
        )
    }

    override def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        import com.pharbers.data.util.sparkDriver.ss.implicits._

        val panelERD = args.getOrElse("panelERD", throw new Exception("not found panelERD"))
        val hospERD = args.getOrElse("hospERD", throw new Exception("not found hospERD"))
        val sourceERD = args.getOrElse("sourceERD", throw new Exception("not found sourceERD"))
        val prodBaseERD = args.getOrElse("prodBaseERD", throw new Exception("not found prodBaseDF"))
        val prodDeliveryERD = args.getOrElse("prodDeliveryERD", Seq.empty[String].toDF("_id"))
        val prodDosageERD = args.getOrElse("prodDosageERD", Seq.empty[String].toDF("_id"))
        val prodMoleERD = args.getOrElse("prodMoleERD", Seq.empty[String].toDF("_id"))
        val prodPackageERD = args.getOrElse("prodPackageERD", Seq.empty[String].toDF("_id"))
        val prodCorpERD = args.getOrElse("prodCorpERD", Seq.empty[String].toDF("_id"))

        val prodDIS = ProdConversion().toDIS(Map(
            "prodBaseERD" -> prodBaseERD,
            "prodDeliveryERD" -> prodDeliveryERD,
            "prodDosageERD" -> prodDosageERD,
            "prodMoleERD" -> prodMoleERD,
            "prodPackageERD" -> prodPackageERD,
            "prodCorpERD" -> prodCorpERD
        ))("prodDIS")

        val panelDIS = panelERD
                .join(hospERD.withColumnRenamed("_id", "hosp_id"), panelERD("HOSP_ID") === hospERD("hosp_id"), "left")
                .drop("hosp_id")
                .join(sourceERD.withColumnRenamed("_id", "source_id"), panelERD("SOURCE_ID") === sourceERD("source_id"), "left")
                .drop("source_id")
                .join(prodDIS.withColumnRenamed("_id", "PROD_ID")
                    , panelERD("MIN_PRODUCT") === (prodDIS("product-name") + prodDIS("dosage") + prodDIS("package-des") + prodDIS("package-number") + prodDIS("corp-name"))
                    , "left")
                .drop("MIN_PRODUCT")
        Map(
            "panelDIS" -> panelDIS
        )
    }
}
