import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Date

/**
  * Created by Administrator on 2017/4/19.
  */
object TestConsole {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("KafkaSparkMySQL").master("local[8]")
      .getOrCreate()
    val date = new Date(2017,4,26)
    import spark.implicits._
    val streamingInputDF =
      spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "192.168.31.58:9092")
        .option("subscribe", "log_adv")
        .option("startingOffsets", "latest")
        .option("minPartitions", "10")
        .option("failOnDataLoss", "true").load()
    val streamingSelectDF = streamingInputDF
      .select(get_json_object(($"value").cast("string"), "$.zone_id").alias("zone_id"),
        get_json_object(($"value").cast("string"), "$.create_time").alias("create_time"))
      .groupBy($"zone_id",window(unix_timestamp($"create_time"), "1 day"))
      .count()
      .select($"zone_id",$"count",date_format($"window.start","yyyy-MM-dd") as "pt")
    val query = streamingSelectDF.writeStream.format("console").outputMode("complete").start()
    query.awaitTermination()
  }
}
