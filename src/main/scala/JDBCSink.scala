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
  def open(partitionId: Long,version: Long): Boolean = { //每个partition于mysql建立连接
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
    if(midCount.isValid && midCount.isExpire) {//检查记录是否合法和记录是否过期

      //logger.error(s"JDBCSinkOutput:${midCount.eventFieldName},${midCount.count}")
      statement.executeUpdate(midCount.mergerStatements)  //插入或者更新数据
      /*val updateRows = statement.executeUpdate(midCount.updateStatement)
      if (updateRows == 0) {
        val insertRows: Int = statement.executeUpdate(midCount.insertStatements)
      }*/
    }
  }
  def close(errorOrNull: Throwable): Unit = { //关闭连接
    if(connection.nonEmpty)
      connection.get.close
  }
}