package com.pharbers.run

/**
  * @description:
  * @author: clock
  * @date: 2019-04-15 15:07
  */
object TransformIMS extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val ImsAtcFile = "/data/IMS/IMS_PRODUCT_STANDARD_201812/cn_atc_ref_201812_1.txt"
    val ImsMnfFile = "/data/IMS/IMS_PRODUCT_STANDARD_201812/cn_mnf_ref_201812_1.txt"
    val ImsLkpFile = "/data/IMS/IMS_PRODUCT_STANDARD_201812/cn_mol_lkp_201812_1.txt"
    val ImsMolFile = "/data/IMS/IMS_PRODUCT_STANDARD_201812/cn_mol_ref_201812_1.txt"
    val ImsProdFile = "/data/IMS/IMS_PRODUCT_STANDARD_201812/cn_prod_ref_201812_1.txt"

    val atcDF = TXT2DF(ImsAtcFile) //789
//    atcDF.show(false)
    val mnfDF = TXT2DF(ImsMnfFile) //6762
//    mnfDF.show(false)
    val lkpDF = TXT2DF(ImsLkpFile) //147152
//    lkpDF.show(false)
    val molDF = TXT2DF(ImsMolFile) //20328
//    molDF.show(false)
    val prodBaseDF = TXT2DF(ImsProdFile) //112848
//    prodBaseDF.show(false)

    val ipc = IMSProductConversion()
    val ImsERD = ipc.toERD(Map(
        "prodBaseDF" -> prodBaseDF
        , "mnfDF" -> mnfDF
        , "lkpDF" -> lkpDF
        , "molDF" -> molDF
    ))("ImsERD")
    ImsERD.show(false)
    println(ImsERD.count())
}
