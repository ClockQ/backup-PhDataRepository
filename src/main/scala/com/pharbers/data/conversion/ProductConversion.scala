package com.pharbers.data.conversion
import org.apache.spark.sql.DataFrame

/**
  * @description: product of calc
  * @author: clock
  * @date: 2019-04-15 14:49
  */
case class ProductConversion() extends PhDataConversion {

    override def toERD(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val prodIMSDF = args.getOrElse("prodIMSDF", throw new Exception("not found prodIMSDF"))

        val productERD = prodIMSDF

        Map(
            "productERD" -> productERD
        )
    }

    override def toDIS(args: Map[String, DataFrame]): Map[String, DataFrame] = {
        val productERD = args.getOrElse("productERD", throw new Exception("not found productERD"))
        val prodIMSERD = args.getOrElse("prodIMSERD", throw new Exception("not found prodIMSERD"))

        val productDIS = productERD.join(prodIMSERD, productERD("") === prodIMSERD(""))

        Map(
            "productDIS" -> productDIS
        )
    }
}
