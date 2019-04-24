package com.pharbers.data.conversion.hosp

import org.apache.spark.sql.DataFrame

/**
  * @ ProjectName pharbers-data-repository.com.pharbers.common.phDataTraie
  * @ author jeorch
  * @ date 19-3-26
  * @ Description: TODO
  */
trait phDataTrait {
    def getDataFromDF(df: DataFrame): Unit
}
