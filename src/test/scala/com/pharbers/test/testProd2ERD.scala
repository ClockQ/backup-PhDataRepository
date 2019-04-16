package com.pharbers.test

object testProd2ERD extends App {

    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._
    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    val pfizer_source_id = "5ca069e2eeefcc012918ec73"

    val prodCvs = ProdConversion()

    val IMS_ATC_DF = TXT2DF(IMS_ATC_LOCATION)
    val IMS_LKP_DF = TXT2DF(IMS_LKP_LOCATION)
    val IMS_MNF_DF = TXT2DF(IMS_MNF_LOCATION)
    val IMS_MOL_DF = TXT2DF(IMS_MOL_LOCATION)
    val IMS_PROD_DF = TXT2DF(IMS_PROD_LOCATION)

    println("IMS_ATC_DF.count = " + IMS_ATC_DF.count())
    println("IMS_LKP_DF.count = " + IMS_LKP_DF.count())
    println("IMS_MNF_DF.count = " + IMS_MNF_DF.count())
    println("IMS_MOL_DF.count = " + IMS_MOL_DF.count())
    println("IMS_PROD_DF.count = " + IMS_PROD_DF.count())

    val DIST_PROD_DF = IMS_PROD_DF.select("Pack_Id", "Prd_desc", "MNF_ID", "Pck_Desc", "Str_Desc", "PckVol_Desc", "PckSize_Desc").distinct()//112848
    val DIST_MNF_DF = IMS_MNF_DF.select("MNF_ID", "Mnf_Desc").distinct()
    val DIST_PackMoleID_DF = IMS_LKP_DF.select("Pack_ID", "Molecule_ID").distinct()
    val DIST_MOL_DF = IMS_MOL_DF.select("Molecule_Id", "Molecule_Desc").distinct()

    val DIS_MIS_PROD_DF = DIST_PROD_DF
        .join(DIST_MNF_DF.withColumnRenamed("MNF_ID", "MATCH_ID"), col("MNF_ID") === col("MATCH_ID"), "left").drop("MATCH_ID")//112848
        .join(DIST_PackMoleID_DF.withColumnRenamed("Pack_ID", "MATCH_ID"), col("Pack_ID") === col("MATCH_ID"), "left").drop("MATCH_ID")//147012
        .join(DIST_MOL_DF.withColumnRenamed("Molecule_Id", "MATCH_ID"), col("Molecule_Id") === col("MATCH_ID"), "left").drop("MATCH_ID")//147012

    val IMS_REF_DF = DIS_MIS_PROD_DF
        .generateProdName
        .generateMoleName
        .generatePackDes
        .generatePackNumber
        .generateCorpName
        .generateDosage

    println("IMS_REF_DF.count = " + IMS_REF_DF.count())


}
