package com.pharbers.data.qitest

import scala.io.Source

/**
  * @description:
  * @author: clock
  * @date: 2019-05-05 12:34
  */
object testReflectText extends App {
    val fileContents = Source.fromFile("src/test/resources/cleanAlgorithm.txt").getLines.mkString(";")
    println(fileContents)
    reflectByText(fileContents)

//    println(reflectByText("1 to 3 map (_+1)"))
//    println(reflectByText(s"""println("abc")"""))
//    reflectByText(s"""com.pharbers.data.qitest.devTest.main(Array("TRUE"))""")

    def reflectByText(arg: String): Any = {
        import scala.tools.reflect.ToolBox
        val tb = scala.reflect.runtime.currentMirror.mkToolBox()
        val tree = tb.parse(arg)
        tb.eval(tree)
    }
}

