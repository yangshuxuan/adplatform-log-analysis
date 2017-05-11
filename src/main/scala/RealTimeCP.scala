package com.adups
import java.util.TimeZone


import grizzled.slf4j.Logger
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import Utility._
/**
  * Created by Administrator on 2017/5/5.
  */
abstract class RealTimeCP[T](val dataFrame: Dataset[T]) {

  val allFieldNames = List("proversion", "reportTag", "mac", "channel",
    "ad_type", "android_id", "provider", "model",
    "gaid", "os_version", "event", "imei", "mid",
    "packageName", "brand", "oper", "location_id",
    "modver", "language", "resolution", "sdk_version",
    "apn", "app_id", "ad_id", "time", "server_time","platform", "vendor", "value",
    "data_source", "intever", "click_effect")

  def groupByFieldNames:List[String]

  val timeFieldName = "server_time"

  val userFieldName = "imei"

  def userColumn = dataFrame(userFieldName)

  val usedFieldNames = userFieldName::timeFieldName::groupByFieldNames

  val schema = StructType(allFieldNames.map(fieldName => StructField(fieldName,StringType,true)))
  val jsonName = "log_adv"
  val countFieldName="count"
  def countColumn = dataFrame(countFieldName)
  private def columns(fieldNames:List[String])={
    fieldNames.map(s => dataFrame(jsonName + "." + s))
  }

  def usedColumns={
    usedFieldNames.map(s => dataFrame(jsonName + "." + s))

  }
  def groupByColumns={
    groupByFieldNames.map(s => dataFrame(s))
  }
  def timeRangeColumn=window(dataFrame(timeFieldName),"1 day")

  def extractJson() = {

    dataFrame.select(from_json(dataFrame("value").cast("string"),schema) as jsonName)
  }
  def selectUsedColumns={
    dataFrame.select(usedColumns:_*)
  }
  def castServerTime={
    dataFrame.withColumn(timeFieldName,dataFrame(timeFieldName).cast("timestamp") )
  }
  def stats={
    val g = timeRangeColumn::groupByColumns
    dataFrame.groupBy(g:_*).agg(approx_count_distinct(userColumn) as countFieldName)
  }

  def outputCPMidCount[U](implicit e:Encoder[U])={

    val ptColumn = date_format(dataFrame("window.start"),"yyyy-MM-dd") as "pt"
    val g = groupByColumns :+ countColumn :+ ptColumn
    dataFrame.select(g:_*).as[U]
  }
  def outputMysql(writer:ForeachWriter[T],checkpointLocation:String,triggerTime:String)={
    dataFrame
      .writeStream
      .option("checkpointLocation", checkpointLocation)
      .foreach(writer)
      .outputMode("complete")
      .trigger(ProcessingTime(triggerTime))
      .start()
  }
}
abstract class CPProcess[U <: MidCount]  {
  TimeZone.setDefault(TimeZone.getTimeZone("GMT+0")) //由于窗口函数是以UTC为计算单位的，而不是以当前时区为计算单位
  @transient lazy val logger = Logger[this.type]

  implicit def toRealTimeCp[T](dataFrame: Dataset[T]):RealTimeCP[T]

  val checkpointLocation:String
  val triggerTime:String

  def writer:JDBCSink[U]
  implicit def encoder:Encoder[U]

  def createKafkaStream()(implicit spark:SparkSession) ={
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "dn120:19090,dn121:19091,dn122:19092")
      .option("subscribe", "log_adv")
      .option("startingOffsets", "latest")
      .option("maxPartitions", 10)
      .option("kafkaConsumer.pollTimeoutMs", 512)
      .option("failOnDataLoss", false).load()
  }
  def run() {
    logger.info("Begin Running Spark Stream")
    while(true) {
      try {
        val query = createKafkaStream()
          .extractJson
          .selectUsedColumns
          .castServerTime
          .stats
          .outputCPMidCount[U]
          .outputMysql(writer,checkpointLocation,triggerTime)
        query.awaitTermination()
      } catch {
        case ex:Throwable => logger.error("spark stream:" + ex.getMessage())
      }
    }
  }
  def main(args: Array[String]): Unit = {
    run()
  }
}