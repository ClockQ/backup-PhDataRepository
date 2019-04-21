package com.pharbers.run

object TransformOadAndAtc3Table extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val chcFile = "/test/OAD CHC data for 5 cities to 2018Q3 v3.csv"
    val chcDF = CSV2DF(chcFile)

    val result = OadAndAtc3TableConversion().toERD(Map("chcDF" -> chcDF))

    val oadERD = result("oadERD")
    oadERD.show(false)
    if (args.isEmpty || args(0) == "TRUE") {
        oadERD.save2Parquet(PROD_OADTABLE_LOCATION)
        oadERD.save2Mongo(PROD_OADTABLE_LOCATION.split("/").last)
    }

    val atc3ERD = result("atc3ERD")
    atc3ERD.show(false)
    if (args.isEmpty || args(0) == "TRUE") {
        atc3ERD.save2Parquet(PROD_ATC3TABLE_LOCATION)
        atc3ERD.save2Mongo(PROD_ATC3TABLE_LOCATION.split("/").last)
    }
}
