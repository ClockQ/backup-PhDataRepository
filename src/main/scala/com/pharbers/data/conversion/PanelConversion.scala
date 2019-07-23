package com.pharbers.data.conversion

import com.pharbers.data.util._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.data.util.commonUDF.generateIdUdf
import com.pharbers.pactions.actionbase.MapArgs
import com.pharbers.spark.phSparkDriver

case class PanelConversion(company_id: String)(implicit val sparkDriver: phSparkDriver) extends PhDataConversion {

    def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val panelDF = args.getOrElse("panelDF", throw new Exception("not found panelDF"))
        val hospDF = args.getOrElse("hospDF", throw new Exception("not found hospDF"))
                .filter(col("PHAIsRepeat") === "0")
        val sourceDF = args.getOrElse("sourceDF", throw new Exception("not found sourceDF"))
        val phaDF = args.getOrElse("phaDF", throw new Exception("not found phaDF"))

        val connHospSource = panelDF.withColumnRenamed("HOSP_ID", "PHA_ID_old")
                .withColumn("source", lit(company_id))
                .join(phaDF, col("PHA_ID_old") === phaDF("PHA_ID"))
                .join(hospDF.withColumnRenamed("_id", "HOSP_ID"), col("PHA_ID_NEW") === hospDF("PHAHospId"), "left")
                .join(sourceDF.withColumnRenamed("_id", "SOURCE_ID"),
                    col("source") === sourceDF("COMPANY_ID") && panelDF("DOI") === sourceDF("MARKET"), "left")

        val notConnHospOfPanel = connHospSource.filter(col("HOSP_ID").isNull)
        val notConnHospOfPanelCount = notConnHospOfPanel.count()
        if (notConnHospOfPanelCount != 0) {
            phDebugLog(notConnHospOfPanelCount + "条医院未匹配, 重新转换")
            val notConnHospDIS = notConnHospOfPanel.select("PHA_ID_NEW", "Hosp_name")
                    .distinct()
                    .withColumn("_id", generateIdUdf())
                    .withColumnRenamed("PHA_ID_NEW", "PHAHospId")
                    .withColumnRenamed("Hosp_name", "title")
                    .cache()

            return toERD(args +
                    ("hospDF" -> hospDF.unionByName(notConnHospDIS.alignAt(hospDF)))
            )
        }

        val notConnSourceOfPanel = connHospSource.filter(col("SOURCE_ID").isNull)
        val notConnSourceOfPanelCount = notConnSourceOfPanel.count()
        if (notConnSourceOfPanelCount != 0) {
            phDebugLog(notConnSourceOfPanelCount + "条source未匹配, 重新转换")
            val notConnSourceDIS = notConnSourceOfPanel.select("source", "DOI")
                    .distinct()
                    .withColumn("_id", generateIdUdf())
                    .withColumnRenamed("source", "COMPANY_ID")
                    .withColumnRenamed("DOI", "MARKET")
                    .cache()

            return toERD(args +
                    ("sourceDF" -> sourceDF.unionByName(notConnSourceDIS.alignAt(sourceDF)))
            )
        }

        val panelERD = connHospSource
                .generateId
                .select("_id", "SOURCE_ID", "Date", "HOSP_ID", "Prod_Name", "Sales", "Units")
                .withColumnRenamed("Date", "DATE")
                .withColumnRenamed("Prod_Name", "MIN_PRODUCT")
                .withColumnRenamed("Sales", "SALE")
                .withColumnRenamed("Units", "UNIT")
        Map(
            "panelERD" -> panelERD,
            "hospDIS" -> hospDF,
            "sourceERD" -> sourceDF
        )
    }

    def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {

        val panelERD = args.getOrElse("panelERD", throw new Exception("not found panelERD"))
        val sourceERD = args.getOrElse("sourceERD", throw new Exception("not found sourceERD"))
        val prodDIS = args.getOrElse("prodDIS", throw new Exception("not found prodDIS"))
                .withColumn("package-des", regexp_replace(col("package-des")," ", ""))
                .withColumn("min", concat(col("product-name"), col("dosage"), col("package-des"), col("package-number"), col("corp-name")))
        val hospDIS = args.getOrElse("hospDIS", throw new Exception("not found hospDIS"))

        val panelDIS = panelERD
                .join(hospDIS.withColumnRenamed("_id", "hosp"), col("HOSP_ID") === col("hosp"), "left")
                .drop("hosp")
                .join(sourceERD.withColumnRenamed("_id", "source"), col("SOURCE_ID") === col("source"), "left")
                .drop("source")
                .join(prodDIS.withColumnRenamed("_id", "PROD_ID"), panelERD("MIN_PRODUCT") === prodDIS("min"), "left")
                .drop("min")
        Map(
            "panelDIS" -> panelDIS
        )
    }

    override def toERD(args: MapArgs): MapArgs = ???

    override def toDIS(args: MapArgs): MapArgs = ???
}
