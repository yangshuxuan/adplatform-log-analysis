/**
  * Created by Administrator on 2017/5/5.
  */
package com.adups

import org.joda.time.LocalDate
import Utility._
//case class无法继承，所以这里是抽象类
abstract class MidCount{
  val location_id:String    //将要输出到Mysql所有字段对应的值
  val app_id:String
  val provider:String
  val ad_id:String
  val ad_type:String
  val data_source:String
  val event:String
  val count:Long
  val pt:String
  val tableName:String
  def eventFieldName = s"event${event}"   //根据事件名确定事件字段
  val eventNames = "100,101,102,103,801,803,802,804".split(",").toSet
  def isValid =  eventNames(event)   //事件名必须如上所列
  def isExpire = (new LocalDate(pt) compareTo new LocalDate().minusDays(2)) > 0   //由于统计结果包括历史数据，因此设置两天内的数据才更新
  def updateStatement =
    s"""update ${tableName} set $eventFieldName = ${count}
        |where location_id = "${location_id}" and app_id = "${app_id}"
        |and pt = "${pt}" and provider ="${provider}" and ad_id="${ad_id}"
        |and ad_type="${ad_type}" and data_source="${data_source}"""".stripMargin

  def insertStatements =
    s"""INSERT INTO ${tableName}(location_id,app_id,provider,ad_id,ad_type,data_source,$eventFieldName,pt)
        |VALUES("${location_id}","${app_id}","${provider}","${ad_id}","${ad_type}","${data_source}",${count},"${pt}")""".stripMargin

  //插入语句，如果记录已经有，那就更新相应字段的值，除了事件字段，其他所有字段建立了索引，这是必须的
  def mergerStatements = s"""INSERT INTO ${tableName}(location_id,app_id,provider,ad_id,ad_type,data_source,
                             |${eventFieldName},pt)
                             |VALUES("${location_id}","${app_id}","${provider}",
                             |"${ad_id}","${ad_type}","${data_source}",${count},"${pt}")
                             |ON DUPLICATE KEY UPDATE  ${eventFieldName} = ${count} """.stripMargin

}
//这个类代表统计结果记录
case class CPMidCount(location_id:String,app_id:String,
                      provider:String,ad_id:String,ad_type:String,data_source:String,
                      event:String,count:Long,pt:String) extends MidCount{
  val tableName = "data_cp_mid_count"

}
//由于apk和gs不再单独统计了，以下两个类作废
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