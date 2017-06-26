package com.adups
import java.util.Properties

/**
  * Created by Administrator on 2017/6/5.
  */
case class MySqlKafka(url:String,user:String,pwd:String,bootstrap:String,topic:String)

object QueueConfig {

  val logAdvConf = MySqlKafka("jdbc:mysql://192.168.1.87:33061/adCenter",
    "adcUsr",
    "buzhi555&$collect%#DAO2017",
    "dn120:19090,dn121:19091,dn122:19092",
    "log_adv")
}

