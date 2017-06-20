/**
  * Created by Administrator on 2017/6/19.
  */
package com.adups

import java.util.TimeZone


import grizzled.slf4j.Logger
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types._
import java.sql.{DriverManager, Connection}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem,Path}
import java.net.URI
import org.apache.hadoop.fs.Path
import Utility._
import org.joda.time.LocalDateTime
object LogAdvSalesCount {
  import spark.implicits._
  abstract class Result{
    def tableName:String

    def targetField:String
    def count:Long
    def pt:String
    def mergerStatements:String=s"""INSERT INTO ${tableName}(${targetField},pt)
                                    |VALUES(${count},"${pt}")
                                    |ON DUPLICATE KEY UPDATE  ${targetField} = ${count} """.stripMargin
  }
  abstract class ResultAppId extends Result{

    val tableName = "report_user_count_appId_date"
    def app_id:String
    override def mergerStatements:String=s"""INSERT INTO ${tableName}(app_id,${targetField},pt)
                                    |VALUES(${app_id},${count},"${pt}")
                                    |ON DUPLICATE KEY UPDATE  ${targetField} = ${count} """.stripMargin
  }
  case class NewUsersAppId(app_id:String,count:Long,pt:String) extends ResultAppId{
    val targetField = "new_user_count"
  }
  case class NewUsersNoAppId(count:Long,pt:String) extends Result{
    val tableName = "report_user_count_date"
    val targetField = "new_user_count"
  }
  case class ActiveUsersAppId(app_id:String,count:Long,pt:String) extends ResultAppId{
    val targetField = "active_user_count"
  }
  case class ActiveUsersNoAppId(count:Long,pt:String) extends Result{
    val tableName = "report_user_count_date"
    val targetField = "active_user_count"
  }
  implicit val newUsersEncoderAppId = Encoders.product[NewUsersAppId]
  implicit val activeUsersEncoderAppId = Encoders.product[ActiveUsersAppId]
  implicit val newUsersEncoderNoAppId = Encoders.product[NewUsersNoAppId]
  implicit val activeUsersEncoderNoAppId = Encoders.product[ActiveUsersNoAppId]
  implicit class A(dataFrame: DataFrame){
    def getCountAppId={
      dataFrame.groupBy("app_id").agg(countDistinct($"imei") as "count")
    }
    def getCountNoAppId={
      dataFrame.groupBy().agg(countDistinct($"imei") as "count")
    }
    def output[T <: Result](pt:String)(implicit e:Encoder[T])={
      val f = dataFrame.withColumn("pt",lit(pt)).as[T]
      f.foreachPartition(iter => {
        withConnection { connection =>
          val statement = connection.createStatement
          iter.foreach { t => {
            statement.executeUpdate(t.mergerStatements)}}
        }
      })

    }
  }
  val driver = "com.mysql.jdbc.Driver"
  val MySqlKafka(url,user,pwd,_,_)=QueueConfig.logAdvConf
  val fileSystem = FileSystem.get(URI.create("hdfs://adups"),new Configuration())

  def withConnection(op:Connection=>Unit): Unit = {
    Class.forName (driver)
    val connection = DriverManager.getConnection (url, user, pwd)
    try {
      op (connection)
    } catch {
      case e: Exception => e.printStackTrace
    } finally {
      connection.close ()
    }
  }
  def dealUsers(path:String,pt:String): Unit ={
    if(fileSystem.exists(new Path(path))) {
      val df = spark.read.parquet(path).select($"imei", when($"app_id".isNull,"0").otherwise($"app_id") as "app_id", $"pt")
      val oldDf = df.filter( $"pt" < pt).select($"imei",$"app_id")
      val curDf = df.filter( $"pt" === pt).select($"imei",$"app_id")
      (curDf except oldDf).getCountAppId.output[NewUsersAppId](pt)
      curDf.getCountAppId.output[ActiveUsersAppId](pt)

      val oldDfNoAppId = df.filter( $"pt" < pt).select($"imei")
      val curDfNoAppId = df.filter( $"pt" === pt).select($"imei")
      (curDfNoAppId except oldDfNoAppId).getCountNoAppId.output[NewUsersNoAppId](pt)
      curDfNoAppId.getCountNoAppId.output[ActiveUsersNoAppId](pt)

    }
  }

  def main(args: Array[String]) {
    val Array(path,pt) = args
    dealUsers(path,pt)
  }
}
