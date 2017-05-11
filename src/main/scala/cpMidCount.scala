/**
  * Created by Administrator on 2017/5/5.
  */
package com.adups

import org.joda.time.LocalDate
import Utility._
abstract class MidCount{
  def event:String
  def pt:String
  def eventFieldName = s"event${event}"
  val eventNames = "100,101,102,103,801,803,802,804".split(",").toSet
  def isValidEvent =  eventNames(event)
  def isExpire = new LocalDate(pt) > new LocalDate().minusDays(2)
  def updateStatement:String
  def insertStatements:String
}
case class CPMidCount(location_id:String,app_id:String,
                      provider:String,ad_id:String,
                      event:String,count:Long,pt:String) extends MidCount{
  val tableName = "data_cp_mid_count"
  def updateStatement =
    s"""update ${tableName} set $eventFieldName = ${count}
        |where location_id = "${location_id}" and app_id = "${app_id}"
        |and pt = "${pt}" and provider ="${provider}" and ad_id="${ad_id}"""".stripMargin

  def insertStatements =
    s"""INSERT INTO ${tableName}(location_id,app_id,provider,ad_id,$eventFieldName,pt)
        |VALUES("${location_id}","${app_id}","${provider}","${ad_id}",${count},"${pt}")""".stripMargin
}
