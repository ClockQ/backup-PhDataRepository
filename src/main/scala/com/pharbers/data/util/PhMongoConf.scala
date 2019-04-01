package com.pharbers.data.util

import com.pharbers.baseModules.PharbersInjectModule

/**
  * @description:
  * @author: clock
  * @date: 2019-03-28 19:54
  */
trait PhMongoConf extends PharbersInjectModule {
    override val id: String = "mongodb-connect"
    override val configPath: String = "pharbers_config/mongodb-config.xml"
    override val md = "server_host" :: "server_port" :: "connect_name" :: "connect_pwd" :: "conn_name" :: Nil

    def conn_name: String = config.mc.find(p => p._1 == "conn_name").get._2.toString

    def server_host: String = config.mc.find(p => p._1 == "server_host").get._2.toString

    def server_port: Int = config.mc.find(p => p._1 == "server_port").get._2.toString.toInt
}

object PhMongoConf extends PhMongoConf