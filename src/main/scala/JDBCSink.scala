/**
  * Created by Administrator on 2017/4/19.
  */
package com.adups
import java.sql._
import org.apache.spark.sql.ForeachWriter
import org.joda.time.LocalDate
import Utility._
import grizzled.slf4j.Logger

class JDBCSink[T <: MidCount] (url:String, user:String, pwd:String) extends ForeachWriter[T] {
  @transient lazy val logger = Logger[this.type]
  val driver = "com.mysql.jdbc.Driver"
  var connection:Option[Connection] = None
  var statement:Statement = _
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
  def process(midCount: T): Unit = {
    if(midCount.isValid && midCount.isExpire) {

      //logger.error(s"JDBCSinkOutput:${midCount.eventFieldName},${midCount.count}")
      val updateRows = statement.executeUpdate(midCount.updateStatement)
      if (updateRows == 0) {
        val insertRows: Int = statement.executeUpdate(midCount.insertStatements)
      }
    }
  }
  def close(errorOrNull: Throwable): Unit = {
    if(connection.nonEmpty)
      connection.get.close
  }
}