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

    val atc = TXT2DF(IMS_ATC_LOCATION)
    atc.show(false)
    val mnf = TXT2DF(IMS_MNF_LOCATION)
    mnf.show(false)
    val lkp = TXT2DF(IMS_LKP_LOCATION)
    lkp.show(false)
    val mol = TXT2DF(IMS_MOL_LOCATION)
    mol.show(false)
    val prod = TXT2DF(IMS_PROD_LOCATION)
    prod.show(false)
}
