package com.pharbers.data.util

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 17:29
  */
object ParquetLocation {
    // hospital
    val HOSP_BASE_LOCATION = "/repository/hosp"
    val HOSP_BED_LOCATION = "/test/hosp/bed"
    val HOSP_ESTIMATE_LOCATION = "/test/hosp/estimate"
    val HOSP_OUTPATIENT_LOCATION = "/test/hosp/outpatient"
    val HOSP_REVENUE_LOCATION = "/test/hosp/revenue"
    val HOSP_SPECIALTY_LOCATION = "/test/hosp/specialty"
    val HOSP_STAFFNUM_LOCATION = "/test/hosp/staffNum"
    val HOSP_UNIT_LOCATION = "/test/hosp/unit"
    // hospital address
    val HOSP_ADDRESS_BASE_LOCATION = "/repository/address"
    val HOSP_ADDRESS_CITY_LOCATION = "/repository/city"
    val HOSP_ADDRESS_MEDLE_LOCATION = "/test/hosp/Address/medle"
    val HOSP_ADDRESS_PREFECTURE_LOCATION = "/repository/prefecture"
    val HOSP_ADDRESS_PROVINCE_LOCATION = "/repository/province"
    val HOSP_ADDRESS_REGION_LOCATION = "/repository/region"
    val HOSP_ADDRESS_TIER_LOCATION = "/repository/tier"
    // hospital pha
    val HOSP_PHA_LOCATION = "/test/hosp/pha"

    // Product Dev
    val PROD_DEV_LOCATION = "/repository/prod_dev"
    // Product IMS
    val PROD_IMS_LOCATION = "/repository/prod_ims"
    // Product ATC
    val PROD_ATCTABLE_LOCATION = "/repository/atcTable"
    val PROD_ATC3TABLE_LOCATION = "/repository/atc3_table"
    val PROD_OADTABLE_LOCATION = "/repository/oad_table"
    // Product Etc
    val PROD_ETC_LOCATION = "/repository/prod_etc"

    // CHC
    val CHC_LOCATION = "/repository/chc"
    val CHC_DATE_LOCATION = "/repository/chc_date"

    //source 可能又问题,先用着
    val SOURCE_LOCATION = "/test/dcs/source.csv"

    //agg
    val MAX_RESULT_MARKET_AGG_LOCATION = "/repository/agg/maxResult/market"
    val MAX_RESULT_PRODUCT_AGG_LOCATION = "/repository/agg/maxResult/product"
}
