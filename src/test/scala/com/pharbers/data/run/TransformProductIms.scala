package com.pharbers.data.run

import com.pharbers.util.log.phLogTrait.phDebugLog
import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

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
    val prodBaseDFCount = prodBaseDF.count()
//    prodBaseDF.show(false)

    val piCvs = ProductImsConversion()

    val productImsERD = piCvs.toERD(MapArgs(Map(
        "prodBaseDF" -> DFArgs(prodBaseDF)
        , "mnfDF" -> DFArgs(mnfDF)
        , "lkpDF" -> DFArgs(lkpDF)
        , "molDF" -> DFArgs(molDF)
    ))).getAs[DFArgs]("productImsERD")
    productImsERD.show(false)

    val productImsERDCount = productImsERD.dropDuplicates("IMS_PACK_ID").count()
    val prodImsMinus = prodBaseDFCount - productImsERDCount
    assert(prodImsMinus == 0, "prodIms: 转换后的ERD比源数据减少`" + prodImsMinus + "`条记录")

    if(args.nonEmpty && args(0) == "TRUE")
        productImsERD.save2Mongo(PROD_IMS_LOCATION.split("/").last).save2Parquet(PROD_IMS_LOCATION)

    val productImsDIS = piCvs.toDIS(MapArgs(Map(
        "productImsERD" -> DFArgs(Parquet2DF(PROD_IMS_LOCATION))
        , "atc3ERD" -> DFArgs(Parquet2DF(PROD_ATC3TABLE_LOCATION))
        , "oadERD" -> DFArgs(Parquet2DF(PROD_OADTABLE_LOCATION))
        , "productDevERD" -> DFArgs(Parquet2DF(PROD_DEV_LOCATION))
    ))).getAs[DFArgs]("productImsDIS")
    productImsDIS.filter(col("DEV_PRODUCT_ID").isNotNull).show(false)

    val imsOadIsNull = productImsDIS.filter(col("OAD_TYPE").isNull)
    imsOadIsNull.show(false)
    phDebugLog(imsOadIsNull.count() == 0, "IMS 产品有" + imsOadIsNull.count() + "条未匹配到OAD")

    val devIdIsNull = productImsDIS.filter(col("DEV_PRODUCT_ID").isNull)
    devIdIsNull.show(false)
    phDebugLog(devIdIsNull.count() == 0, "IMS 产品有" + devIdIsNull.count() + "条未匹配到 DEV_PRODUCT")
}
