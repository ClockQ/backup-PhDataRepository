package com.pharbers.test

import com.pharbers.data.conversion.{HospConversion, MaxResultConversion, ProdConversion}
import com.pharbers.data.util.Parquet2DF
import com.pharbers.data.util.ParquetLocation._

class testMaxResultConversionJob extends App{
    val hospCvs = HospConversion()
    val prodCvs = ProdConversion()
    val pfizerInfMaxCvs = MaxResultConversion("")
    val prod_base_location = "/test/dcs/5b028f95ed925c2c705b85ba-201901-INF"

    val hospDIS = hospCvs.toDIS(
        Map(
            "hospBaseERD" -> Parquet2DF(HOSP_BASE_LOCATION),
            "hospAddressERD" -> Parquet2DF(HOSP_ADDRESS_BASE_LOCATION),
            "hospPrefectureERD" -> Parquet2DF(HOSP_ADDRESS_PREFECTURE_LOCATION),
            "hospCityERD" -> Parquet2DF(HOSP_ADDRESS_CITY_LOCATION),
            "hospProvinceERD" -> Parquet2DF(HOSP_ADDRESS_PROVINCE_LOCATION)
        )
    )("hospDIS")
    val prodDIS = prodCvs.toDIS(
        Map(
            "prodBaseERD" -> Parquet2DF(PROD_BASE_LOCATION),
            "prodDeliveryERD" -> Parquet2DF(PROD_DELIVERY_LOCATION),
            "prodDosageERD" -> Parquet2DF(PROD_DOSAGE_LOCATION),
            "prodMoleERD" -> Parquet2DF(PROD_MOLE_LOCATION),
            "prodPackageERD" -> Parquet2DF(PROD_PACKAGE_LOCATION),
            "prodCorpERD" -> Parquet2DF(PROD_CORP_LOCATION)
        )
    )("prodDIS")

    val maxDIS = pfizerInfMaxCvs.toDIS(
        Map(
            "maxERD" -> Parquet2DF(prod_base_location),
            "hospDIS" -> hospDIS,
            "prodDIS" -> prodDIS
        )
    )("maxDIS")
}
