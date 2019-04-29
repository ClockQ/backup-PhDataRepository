package com.pharbers.data.run

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

object TransformOadAndAtc3Table extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.data.util.ParquetLocation._

    val chcFile_Q3 = "/test/OAD CHC data for 5 cities to 2018Q3 v3.csv"
    val chcFile_Q4 = "/test/chc/OAD CHC data for 5 cities to 2018Q4.csv"

    val chcDF = CSV2DF(chcFile_Q3).unionByName(CSV2DF(chcFile_Q4))

    val result = OadAndAtc3TableConversion().toERD(MapArgs(Map("chcDF" -> DFArgs(chcDF))))

    val oadERD = result.get("oadERD").getBy[DFArgs]
    oadERD.show(false)
    if(args.nonEmpty && args(0) == "TRUE")
        oadERD.save2Parquet(PROD_OADTABLE_LOCATION).save2Mongo(PROD_OADTABLE_LOCATION.split("/").last)

    val atc3ERD = result.get("atc3ERD").getBy[DFArgs]
    atc3ERD.show(false)
    if(args.nonEmpty && args(0) == "TRUE")
        atc3ERD.save2Parquet(PROD_ATC3TABLE_LOCATION).save2Mongo(PROD_ATC3TABLE_LOCATION.split("/").last)
}
