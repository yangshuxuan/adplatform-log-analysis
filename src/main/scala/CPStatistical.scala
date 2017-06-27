/**
  * Created by Administrator on 2017/5/8.
  */
package com.adups

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, SparkSession, Dataset}
import org.apache.spark.sql.Encoders

object CPStatistical extends CPProcess[CPMidCount]{ //main函数在父类里面，先前有三个统计，为了代码复用而这样的设计的
  val url="jdbc:mysql://192.168.1.87:33061/adCenter"   //指定统计结果存放的数据库
  val user ="adcUsr"
  val pwd = """buzhi555&$collect%#DAO2017"""
  //val checkpointLocation = "/user/newspark/realtimesynccheckpoint/cp13"
  //val triggerTime = "15 seconds"
  implicit def toRealTimeCp[T](dataFrame: Dataset[T]):RealTimeCP[T] = new RealTimeCP[T](dataFrame){
    //指定需要groupBy的字段，cp目前就是下面的一些字段
    def groupByFieldNames:List[String] = List("location_id","app_id","provider","ad_id","ad_type","data_source","event")
  }

  def encoder=Encoders.product[CPMidCount] //统计的结果输出之前必须强制转换为特定的类型，此处是CPMidCount，而转换的时候必须指定编码器
}
