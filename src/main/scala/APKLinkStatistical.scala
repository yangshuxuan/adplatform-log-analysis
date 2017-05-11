/**
  * Created by Administrator on 2017/5/8.
  */
package com.adups

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, SparkSession, Dataset}
import org.apache.spark.sql.Encoders

object APKLinkStatistical extends CPProcess[APKLinkMidCount]{
  val url="jdbc:mysql://192.168.1.23:33061/adCenter"
  val user ="adcUsr"
  val pwd = """buzhi555&$collect%#DAO2017"""
  val checkpointLocation = "/user/newspark/realtimesynccheckpoint/cp4"
  val triggerTime = "5 minutes"
  implicit def toRealTimeCp[T](dataFrame: Dataset[T]):RealTimeCP[T] = new RealTimeCP[T](dataFrame){
    def groupByFieldNames:List[String] = List("location_id","app_id","provider","ad_id","ad_type","event")
  }

  def encoder=Encoders.product[APKLinkMidCount]
}
