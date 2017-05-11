/**
  * Created by Administrator on 2017/4/19.
  */
package com.adups

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession, Dataset}


object TryWithTestDataBase  extends CPProcess[CPMidCount] {

  val url="jdbc:mysql://61.160.47.31:3306/adCenter"
  val user ="root"
  val pwd = """fvsh2225"""
  val checkpointLocation = "/user/newspark/other7checkpoint"
  val triggerTime = "10 seconds"
  implicit def toRealTimeCp[T](dataFrame: Dataset[T]):RealTimeCP[T] = new RealTimeCP[T](dataFrame){
    def groupByFieldNames:List[String] = List("location_id","app_id","provider","ad_id","event")

  }
  def writer:JDBCSink[CPMidCount]={
    new JDBCSink[CPMidCount](url,user, pwd)
  }
  def encoder=Encoders.product[CPMidCount]
}
