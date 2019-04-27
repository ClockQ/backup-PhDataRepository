package com.pharbers.data.conversion

import com.pharbers.pactions.actionbase.{DFArgs, MapArgs}

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 16:40
  */
case class HospConversion() extends PhDataConversion {

    import com.pharbers.data.util._
    import org.apache.spark.sql.functions._
    import com.pharbers.data.util.sparkDriver.ss.implicits._

    override def toERD(args: MapArgs): MapArgs = ???

    override def toDIS(args: MapArgs): MapArgs = {
        val hospBaseERD = args.get.getOrElse("hospBaseERD", throw new Exception("not found hospBaseERD")).getBy[DFArgs]
        val hospBedERD = args.get.getOrElse("hospBedERD", Seq.empty[String].toDF("_id"))
        val hospEstimateERD = args.get.getOrElse("hospEstimateERD", Seq.empty[String].toDF("_id"))
        val hospOutpatientERD = args.get.getOrElse("hospOutpatientERD", Seq.empty[String].toDF("_id"))
        val hospRevenueERD = args.get.getOrElse("hospRevenueERD", Seq.empty[String].toDF("_id"))
        val hospSpecialtyERD = args.get.getOrElse("hospSpecialtyERD", Seq.empty[String].toDF("_id"))
        val hospStaffNumERD = args.get.getOrElse("hospStaffNumERD", Seq.empty[String].toDF("_id"))
        val hospUnitERD = args.get.getOrElse("hospUnitERD", Seq.empty[String].toDF("_id"))
        val hospAddressERD = args.get.get("hospAddressERD")
        val hospPrefectureERD = args.get.get("hospPrefectureERD")
        val hospCityERD = args.get.get("hospCityERD")
        val hospProvinceERD = args.get.get("hospProvinceERD")

        val addressDIS = hospAddressERD match {
            case Some(address) =>
                val addressDF = address.getBy[DFArgs]

                val addressJoinPrefecture = hospPrefectureERD match {
                    case Some(prefecture) =>
                        val prefectureDF = prefecture.getBy[DFArgs]
                                .withColumnRenamed("_id", "PREFECTURE_ID")
                                .withColumnRenamed("name", "PREFECTURE_NAME")
                                .withColumnRenamed("polygon", "PREFECTURE_POLYGON")
                        addressDF.join(
                            prefectureDF
                            , addressDF("prefecture") === prefectureDF("PREFECTURE_ID")
                            , "left"
                        )
                    case None => addressDF
                }

                val addressJoinCity = hospCityERD match {
                    case Some(city) =>
                        val cityDF = city.getBy[DFArgs]
                                .withColumnRenamed("_id", "CITY_ID")
                                .withColumnRenamed("name", "CITY_NAME")
                                .withColumnRenamed("polygon", "CITY_POLYGON")
                        addressJoinPrefecture.join(
                            cityDF
                            , addressJoinPrefecture("city") === cityDF("CITY_ID")
                            , "left"
                        )
                    case None => addressJoinPrefecture
                }

                val addressJoinProvince = hospProvinceERD match {
                    case Some(province) =>
                        val provinceDF = province.getBy[DFArgs]
                                .withColumnRenamed("_id", "PROVINCE_ID")
                                .withColumnRenamed("name", "PROVINCE_NAME")
                                .withColumnRenamed("polygon", "PROVINCE_POLYGON")
                        addressJoinCity.join(
                            provinceDF
                            , addressJoinCity("province") === provinceDF("PROVINCE_ID")
                            , "left"
                        )
                    case None => addressJoinCity
                }

                addressJoinProvince

            case None => Seq.empty[String].toDF("_id")
        }

        val hospDIS = {
            hospBaseERD
                    .join(
                        addressDIS,
                        hospBaseERD("addressID") === addressDIS("_id"),
                        "left"
                    ).drop(addressDIS("_id"))
                    .withColumnRenamed("title", "HOSP_NAME")
                    .withColumnRenamed("PHAIsRepeat", "PHA_IS_REPEAT")
                    .withColumnRenamed("PHAHospId", "PHA_HOSP_ID")
                    .withColumnRenamed("type", "HOSP_TYPE")
                    .withColumnRenamed("level", "HOSP_LEVEL")
                    .withColumnRenamed("character", "HOSP_CHARACTER")
        }

        MapArgs(Map("hospDIS" -> DFArgs(hospDIS)))
    }
}
