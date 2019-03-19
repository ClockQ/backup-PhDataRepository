package common

import com.pharbers.spark.phSparkDriver

object phFactory{
    private lazy val sparkDriver: phSparkDriver = phSparkDriver("cui-test")

    def getSparkInstance(): phSparkDriver = sparkDriver
}