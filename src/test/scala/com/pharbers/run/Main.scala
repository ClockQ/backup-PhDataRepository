package com.pharbers.run

/**
  * @description:
  * @author: clock
  * @date: 2019-04-19 18:33
  */
object Main extends App {
    val SAVE = Array("TRUE")
    val NOT_SAVE = Array("FALSE")
    val STATUS = NOT_SAVE

    TransformOadAndAtc3Table.main(STATUS)
    TransformProductIms.main(STATUS)
    TransformAtcTable.main(STATUS)
    TransformMarket.main(STATUS)
    TransformProductEtc.main(STATUS)
    TransformProductDev.main(STATUS)
}
