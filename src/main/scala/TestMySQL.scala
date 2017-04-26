/**
  * Created by Administrator on 2017/4/19.
  */
package com.adups
import org.apache.spark.sql.functions.{get_json_object, json_tuple}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
object TestMySQL {
  val url="jdbc:mysql://61.160.47.31:3306/adCenter"
  val user ="root"
  val pwd = "fvsh2225"
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("KafkaSparkMySQL")
      .getOrCreate()

    import spark.implicits._
    val streamingInputDF: DataFrame =
      spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "192.168.31.58:9092")
        .option("subscribe", "log_adv")
        .option("startingOffsets", "latest")
        .option("maxPartitions", 10)
        .option("kafkaConsumer.pollTimeoutMs", 512)
        .option("failOnDataLoss", false).load()

    val streamingSelectDF: Dataset[(String, Long, String)] =
      streamingInputDF
        .select(get_json_object(($"value").cast("string"), "$.zone_id").alias("zone_id"),
          get_json_object(($"value").cast("string"), "$.create_time").alias("create_time"))
        .groupBy($"zone_id",window($"create_time".cast("timestamp"), "1 day"))
        .count()
        .select($"zone_id",$"count",date_format($"window.start","yyyy-MM-dd") as "pt")
        .as[(String,Long,String)]
    val writer = new JDBCSink(url,user, pwd)
    while(true) {
      try {
        val query = streamingSelectDF
          .writeStream
          .option("checkpointLocation", "/user/hadoop/other2checkpoint")
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
