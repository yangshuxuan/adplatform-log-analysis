/**
  * Created by Administrator on 2017/5/5.
  */
package com.adups

import org.joda.time.LocalDate
import Utility._
abstract class MidCount{
  val location_id:String
  val app_id:String
  val provider:String
  val ad_id:String
  val ad_type:String
  val data_source:String
  val event:String
  val count:Long
  val pt:String
  val tableName:String
  def eventFieldName = s"event${event}"
  val eventNames = "100,101,102,103,801,803,802,804".split(",").toSet
  def isValid =  eventNames(event)
  def isExpire = new LocalDate(pt) > new LocalDate().minusDays(2)
  def updateStatement =
    s"""update ${tableName} set $eventFieldName = ${count}
        |where location_id = "${location_id}" and app_id = "${app_id}"
        |and pt = "${pt}" and provider ="${provider}" and ad_id="${ad_id}"
        |and ad_type="${ad_type}" and data_source="${data_source}"""".stripMargin

  def insertStatements =
    s"""INSERT INTO ${tableName}(location_id,app_id,provider,ad_id,ad_type,data_source,$eventFieldName,pt)
        |VALUES("${location_id}","${app_id}","${provider}","${ad_id}","${ad_type}","${data_source}",${count},"${pt}")""".stripMargin

  def mergerStatements = s"""INSERT INTO ${tableName}(location_id,app_id,provider,ad_id,ad_type,data_source,
                             |${eventFieldName},pt)
                             |VALUES("${location_id}","${app_id}","${provider}",
                             |"${ad_id}","${ad_type}","${data_source}",${count},"${pt}")
                             |ON DUPLICATE KEY UPDATE  ${eventFieldName} = ${count} """.stripMargin

}
case class CPMidCount(location_id:String,app_id:String,
                      provider:String,ad_id:String,ad_type:String,data_source:String,
                      event:String,count:Long,pt:String) extends MidCount{
  val tableName = "data_cp_mid_count"

}
case class APKLinkMidCount(location_id:String,app_id:String,
                      provider:String,ad_id:String,ad_type:String,data_source:String,
                      event:String,count:Long,pt:String) extends MidCount{

  val tableName = s"data_${ad_type}_mid_count"
  override def isValid =  super.isValid && (ad_type=="apk" || ad_type=="link")

}
case class GSMidCount(location_id:String,app_id:String,
                           provider:String,ad_id:String,ad_type:String,data_source:String,
                           event:String,count:Long,pt:String) extends MidCount{
  val tableName = s"data_${data_source}_mid_count"
  override def isValid =  super.isValid && data_source=="gs"

}