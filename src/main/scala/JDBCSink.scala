/**
  * Created by Administrator on 2017/4/19.
  */
package com.adups
import java.sql._
import org.apache.spark.sql.ForeachWriter
import org.joda.time.LocalDate
import Utility._
import grizzled.slf4j.Logger

class JDBCSink (url:String, user:String, pwd:String) extends ForeachWriter[cpMidCount] {
  @transient lazy val logger = Logger[this.type]
  val driver = "com.mysql.jdbc.Driver"
  var connection:Option[Connection] = None
  var statement:Statement = _
  val eventNames = "100,101,102,103,801,803,802,804".split(",").toSet
  def open(partitionId: Long,version: Long): Boolean = {
    Class.forName(driver)
    try {
      connection = Some(DriverManager.getConnection(url, user, pwd))
      statement = connection.get.createStatement
      true
    } catch {
      case ex:Throwable => {
        logger.error("DataBase Error:"+ ex)
        false
      }
    }
  }

  def process(value: cpMidCount): Unit = {
    if(eventNames(value.event) && new LocalDate(value.pt) > new LocalDate().minusDays(2)) {
      val eventFieldName = s"event${value.event}"
      val updateStatement = s"""update cp_mid_count set $eventFieldName = ${value.count} where location_id = "${value.location_id}" and app_id = "${value.app_id}" and pt = "${value.pt}" and provider ="${value.provider}""""
      val updateRows = statement.executeUpdate(updateStatement)
      if (updateRows == 0) {
        val insertStatements =s"""INSERT INTO cp_mid_count(location_id,app_id,provider,$eventFieldName,pt) VALUES("${value.location_id}","${value.app_id}","${value.provider}",${value.count},"${value.pt}")"""
        val insertRows: Int = statement.executeUpdate(insertStatements)
      }
    }

  }

  def close(errorOrNull: Throwable): Unit = {
    if(connection.nonEmpty)
      connection.get.close
  }
}