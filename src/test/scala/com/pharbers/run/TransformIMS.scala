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

    val atc = TXT2DF(ImsAtcFile)
    atc.show(false)
    val mnf = TXT2DF(ImsMnfFile)
    mnf.show(false)
    val lkp = TXT2DF(ImsLkpFile)
    lkp.show(false)
    val mol = TXT2DF(ImsMolFile)
    mol.show(false)
    val prod = TXT2DF(ImsProdFile)
    prod.show(false)

    val ipc = IMSProductConversion()
    ipc.toERD(Map(
        "a" -> atc
    ))
}
