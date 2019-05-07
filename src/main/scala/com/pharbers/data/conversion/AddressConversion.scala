package com.pharbers.data.conversion



/**
  * @description:
  * @author: clock
  * @date: 2019-04-28 16:54
  */
case class AddressConversion() extends PhDataConversion2 {

    import com.pharbers.data.util._
    import com.pharbers.data.util.sparkDriver.ss.implicits._
    import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

    override def toERD(args: MapArgs): MapArgs = ???

    override def toDIS(args: MapArgs): MapArgs = {
        val provinceERD = args.get.get("provinceERD")
        val cityERD = args.get.get("cityERD")
        val prefectureERD = args.get.get("prefectureERD")
        val addressERD = args.get.get("addressERD")

        val provinceDIS = provinceERD match {
            case Some(province) =>
                val provinceDF = province.getBy[DFArgs]
                        .withColumnRenamed("_id", "PROVINCE_ID")
                        .withColumnRenamed("name", "PROVINCE_NAME")
                        .withColumnRenamed("polygon", "PROVINCE_POLYGON")
                provinceDF
            case None => Seq.empty[(String, String, String)].toDF("PROVINCE_ID", "PROVINCE_NAME", "PROVINCE_POLYGON")
        }

        val cityDIS = cityERD match {
            case Some(city) =>
                val cityDF = city.getBy[DFArgs]
                        .withColumnRenamed("_id", "CITY_ID")
                        .withColumnRenamed("name", "CITY_NAME")
                        .withColumnRenamed("polygon", "CITY_POLYGON")
                        .withColumnRenamed("tier", "CITY_TIER")

                cityDF.join(
                    provinceDIS
                    , cityDF("province") === provinceDIS("PROVINCE_ID")
                    , "left"
                ).drop("province")
            case None => provinceDIS
        }

        val prefectureDIS = prefectureERD match {
            case Some(prefecture) =>
                val prefectureDF = prefecture.getBy[DFArgs]
                        .withColumnRenamed("_id", "PREFECTURE_ID")
                        .withColumnRenamed("name", "PREFECTURE_NAME")
                        .withColumnRenamed("polygon", "PREFECTURE_POLYGON")

                prefectureDF.join(
                    cityDIS
                    , prefectureDF("city") === cityDIS("CITY_ID")
                    , "left"
                ).drop("city")
            case None => cityDIS
        }

        val addressDIS = addressERD match {
            case Some(address) =>
                val addressDF = address.getBy[DFArgs]
                        .withColumnRenamed("_id", "ADDRESS_ID")
                        .withColumnRenamed("region", "ADDRESS_REGION")
                        .withColumnRenamed("location", "ADDRESS_LOCATION")
                        .withColumnRenamed("desc", "ADDRESS_DESC")

                addressDF.join(
                    prefectureDIS
                    , addressDF("prefecture") === prefectureDIS("PREFECTURE_ID")
                    , "left"
                ).drop("prefecture")
            case None => prefectureDIS
        }

        MapArgs(Map("addressDIS" -> DFArgs(addressDIS)))
    }
}
