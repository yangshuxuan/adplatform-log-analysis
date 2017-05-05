/**
  * Created by Administrator on 2017/4/19.
  */
package com.adups
import java.sql._
import org.apache.spark.sql.ForeachWriter
import org.joda.time.LocalDate
import Utility._
import grizzled.slf4j.Logger

class JDBCSink (url:String, user:String, pwd:String) extends ForeachWriter[(String,String,String, Long,String)] {
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

  def process(value: (String,String,String,Long,String)): Unit = {
    if(eventNames(value._3) && new LocalDate(value._5) > new LocalDate().minusDays(2)) {
      val eventFieldName = s"event${value._3}"
      val updateStatement = s"""update cp_mid_count set $eventFieldName = ${value._4} where location_id = "${value._1}" and app_id = "${value._2}" and pt = "${value._5}""""
      val updateRows = statement.executeUpdate(updateStatement)
      if (updateRows == 0) {
        val insertStatements =s"""INSERT INTO cp_mid_count(location_id,app_id,$eventFieldName,pt) VALUES("${value._1}","${value._2}",${value._4},"${value._5}")"""
        val insertRows: Int = statement.executeUpdate(insertStatements)
      }
    }

  }

  def close(errorOrNull: Throwable): Unit = {
    if(connection.nonEmpty)
      connection.get.close
  }
}