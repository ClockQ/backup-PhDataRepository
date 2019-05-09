package com.pharbers.data.run

/**
  * @description:
  * @author: clock
  * @date: 2019-04-28 16:41
  */
object TransformAddress extends App {

    import com.pharbers.data.util._
    import com.pharbers.data.conversion._
    import com.pharbers.pactions.actionbase._
    import com.pharbers.data.util.ParquetLocation._

    import com.pharbers.data.util.spark._
    import sparkDriver.ss.implicits._

    val provinceDF = Parquet2DF(HOSP_ADDRESS_PROVINCE_LOCATION)
    val provinceDFCount = provinceDF.count()

    val cityDF = Parquet2DF(HOSP_ADDRESS_CITY_LOCATION)
    val cityDFCount = cityDF.count()

    val prefectureDF = Parquet2DF(HOSP_ADDRESS_PREFECTURE_LOCATION)
    val prefectureDFCount = prefectureDF.count()

    val addressDF = Parquet2DF(HOSP_ADDRESS_BASE_LOCATION)
    val addressDFCount = addressDF.count()

    val addressDIS = AddressConversion().toDIS(MapArgs(Map(
        "provinceERD" -> DFArgs(provinceDF)
        , "cityERD" -> DFArgs(cityDF)
        , "prefectureERD" -> DFArgs(prefectureDF)
        , "addressERD" -> DFArgs(addressDF)
    ))).getAs[DFArgs]("addressDIS")
    val addressDISCount = addressDIS.count()
    addressDIS.show(false)
    println(provinceDFCount, cityDFCount, prefectureDFCount, addressDFCount, addressDISCount)

    if (args.nonEmpty && args(0) == "TRUE")
        addressDIS.save2Parquet(ADDRESS_DIS_LOCATION)//.save2Mongo(ADDRESS_DIS_LOCATION.split("/").last)
}
