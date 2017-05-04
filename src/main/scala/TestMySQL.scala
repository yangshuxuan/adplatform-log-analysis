/**
  * Created by Administrator on 2017/4/19.
  */
package com.adups
import org.apache.spark.sql.functions.{get_json_object, json_tuple}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.util.TimeZone
object TestMySQL {
  val url="jdbc:mysql://192.168.1.23:33061/adCenter"
  val user ="adcUsr"
  val pwd = """buzhi555&$collect%#DAO2017"""
  val schema = StructType(List("proversion", "reportTag", "mac", "channel",
    "ad_type", "android_id", "provider", "model",
    "gaid", "os_version", "event", "imei", "mid",
    "packageName", "brand", "oper", "location_id",
    "modver", "language", "resolution", "sdk_version",
    "apn", "app_id", "ad_id", "time", "platform", "vendor", "value",
    "data_source", "intever", "click_effect").map(fieldName => StructField(fieldName,StringType,true)))
  def main(args: Array[String]) {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT+0")) //由于窗口函数是以UTC为计算单位的，而不是以当前时区为计算单位
    val spark = SparkSession
      .builder
      .appName("KafkaSparkMySQL")
      .getOrCreate()

    import spark.implicits._
    val streamingInputDF: DataFrame =spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "dn120:19090,dn121:19091,dn122:19092")
        .option("subscribe", "log_adv")
        .option("startingOffsets", "latest")
        .option("maxPartitions", 10)
        .option("kafkaConsumer.pollTimeoutMs", 512)
        .option("failOnDataLoss", false).load()
        .select(from_json($"value".cast("string"),schema) as "log_adv")
        .select($"log_adv.app_id",
          $"log_adv.location_id",
          $"log_adv.event",$"log_adv.imei",($"log_adv.time".cast("long")/1000).cast("timestamp") as "time")

    val streamingSelectDF: Dataset[(String,String,String,Long,String)] =
      streamingInputDF
        .groupBy($"location_id",
          $"app_id",
          $"event",
          window($"time","1 day")).agg(approx_count_distinct($"imei") as "count")
        .select($"location_id",$"app_id", $"event",$"count",date_format($"window.start","yyyy-MM-dd") as "pt")
        .as[(String,String,String,Long,String)]
    val writer = new JDBCSink(url,user, pwd)
    while(true) {
      try {
        val query = streamingSelectDF
          .writeStream
          .option("checkpointLocation", "/user/newspark/other2checkpoint")
          .foreach(writer)
          .outputMode("complete")
          .trigger(ProcessingTime("10 seconds"))
          .start()
        query.awaitTermination()
      } catch {
        case ex:Throwable => println("YSX--" + ex.getMessage())
      }
    }
  }
}
