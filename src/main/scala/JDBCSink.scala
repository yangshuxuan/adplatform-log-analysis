/**
  * Created by Administrator on 2017/4/19.
  */
package com.adups
import java.sql._
import org.apache.spark.sql.ForeachWriter
class JDBCSink (url:String, user:String, pwd:String) extends ForeachWriter[(String, Long,String)] {
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
      case ex:Throwable => false
    }
  }

  def process(value: (String, Long,String)): Unit = {
    val updateStatement = s"""update zip_test set total = ${value._2} where zone_id = "${value._1}" and pt = "${value._3}" """
    val updateRows = statement.executeUpdate(updateStatement)
    if(updateRows == 0) {
      val insertStatements =s"""INSERT INTO zip_test(zone_id,total,pt) VALUES("${value._1}",${value._2},"${value._3}")"""
      val insertRows: Int = statement.executeUpdate(insertStatements)
    }

  }

  def close(errorOrNull: Throwable): Unit = {
    if(connection.nonEmpty)
      connection.get.close
  }
}