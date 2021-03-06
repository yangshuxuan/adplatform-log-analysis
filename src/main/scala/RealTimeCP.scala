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
    dataFrame.groupBy(g:_*).agg(count(userColumn) as countFieldName)
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
  //val triggerTime:String
  val url:String
  val user:String
  val pwd:String
  def writer:JDBCSink[U]=new JDBCSink[U](url,user, pwd)
  implicit def encoder:Encoder[U]
  //implicit def encoder=Encoders.product[U]
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
  def run(customTrigerTime:String) {
    logger.info("Begin Running Spark Stream")
    while(true) {
      try {
        val query = createKafkaStream()
          .extractJson
          .selectUsedColumns
          .castServerTime
          .stats
          .outputCPMidCount[U]
          .outputMysql(writer,checkpointLocation,customTrigerTime)
        query.awaitTermination()
      } catch {
        case ex:Throwable => logger.error("spark stream:" + ex.getMessage())
      }
    }
  }
  def main(args: Array[String]): Unit = {
    def errorRemind() {

        System.err.println("You arguments were " + args.mkString("[", ", ", "]"))
        System.err.println(
          """
          |Usage: com.adups.CPStatistical <间隔时间量> <间隔时间单位>.
          |     <间隔时间量> 必须是整数
          |     <间隔时间单位> minutes,seconds,etc.
          |
        """.
            stripMargin
        )
        System.exit(1)

    }
    if (args.length != 2) errorRemind()
    val Array(intervalNum,intervalUnit)=args
    val timeUnitSet = Set("minutes","seconds","hours","days")
    try{
      intervalNum.toInt
      val lowerIntevalUnit = intervalUnit.toLowerCase
      if(! timeUnitSet(lowerIntevalUnit))
        throw new Throwable("Error Unit")
      println(s"$intervalNum $intervalUnit")
      run(s"$intervalNum $intervalUnit")


    }catch{
      case ex:Throwable => {
        logger.error("Parameters Error:"+ ex)
        errorRemind()

      }
    }

  }
}