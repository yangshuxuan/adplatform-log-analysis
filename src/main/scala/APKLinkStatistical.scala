/**
  * Created by Administrator on 2017/5/8.
  */
package com.adups

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, SparkSession, Dataset}
import org.apache.spark.sql.Encoders

object APKLinkStatistical extends CPProcess[APKLinkMidCount]{
  val url=""
  val user =""
  val pwd = """"""
  val checkpointLocation = "/user/newspark/realtimesynccheckpoint/cp11"
  //val triggerTime = "30 seconds"
  implicit def toRealTimeCp[T](dataFrame: Dataset[T]):RealTimeCP[T] = new RealTimeCP[T](dataFrame){
    def groupByFieldNames:List[String] = List("location_id","app_id","provider","ad_id","ad_type","data_source","event")
  }

  def encoder=Encoders.product[APKLinkMidCount]
}
