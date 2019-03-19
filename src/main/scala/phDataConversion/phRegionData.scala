package phDataConversion

import common.phFactory

class phRegionData {
    def getRegionDataFromCsv(): Unit ={
        val driver = phFactory.getSparkInstance()
        val df = driver.ss.read.format("com.databricks.spark.csv")
                .option("header", "true")
                .option("delimiter", ",")
                .load("/test/2019年Universe更新维护1.0.csv")
        
    }
}
