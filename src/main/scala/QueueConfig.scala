package com.adups
import java.util.Properties

/**
  * Created by Administrator on 2017/6/5.
  */
case class MySqlKafka(url:String,user:String,pwd:String,bootstrap:String,topic:String)

object QueueConfig {

  val logAdvConf = MySqlKafka("",
    "",
    "",
    "",
    "log_adv")
}

