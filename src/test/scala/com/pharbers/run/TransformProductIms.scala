package com.pharbers.run

/**
  * @description:
  * @author: clock
  * @date: 2019-04-15 15:07
  */
object TransformProductIms extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.ParquetLocation._

    val ImsMnfFile = "/data/IMS/IMS_PRODUCT_STANDARD_201812/cn_mnf_ref_201812_1.txt"
    val ImsLkpFile = "/data/IMS/IMS_PRODUCT_STANDARD_201812/cn_mol_lkp_201812_1.txt"
    val ImsMolFile = "/data/IMS/IMS_PRODUCT_STANDARD_201812/cn_mol_ref_201812_1.txt"
    val ImsProdFile = "/data/IMS/IMS_PRODUCT_STANDARD_201812/cn_prod_ref_201812_1.txt"

    val mnfDF = TXT2DF(ImsMnfFile) //6762
//    mnfDF.show(false)
    val lkpDF = TXT2DF(ImsLkpFile) //147152
//    lkpDF.show(false)
    val molDF = TXT2DF(ImsMolFile) //20328
//    molDF.show(false)
    val prodBaseDF = TXT2DF(ImsProdFile) //112848
//    prodBaseDF.show(false)

    val piCvs = ProductImsConversion()

    val productImsERD = piCvs.toERD(Map(
        "prodBaseDF" -> prodBaseDF
        , "mnfDF" -> mnfDF
        , "lkpDF" -> lkpDF
        , "molDF" -> molDF
    ))("productImsERD")
    productImsERD.show(false)

    if(args.isEmpty || args(0) == "TRUE") {
        val prodBaseDFCount = prodBaseDF.count()
        val productImsERDCount = productImsERD.dropDuplicates("IMS_PACK_ID").count()
        val prodImsMinus = prodBaseDFCount - productImsERDCount
        assert(prodImsMinus == 0, "prodIms: 转换后的ERD比源数据减少`" + prodImsMinus + "`条记录")

        productImsERD.save2Mongo(PROD_IMS_LOCATION.split("/").last)
        productImsERD.save2Parquet(PROD_IMS_LOCATION)
    }

    val productImsDIS = piCvs.toDIS(Map(
        "productImsERD" -> Parquet2DF(PROD_IMS_LOCATION)
        , "atc3ERD" -> Parquet2DF(PROD_ATC3TABLE_LOCATION)
        , "oadERD" -> Parquet2DF(PROD_OADTABLE_LOCATION)
    ))("productImsDIS")
    productImsDIS.show(false)
    productImsDIS.filter(col("OAD_TYPE").isNull).show(false)
}
