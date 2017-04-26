import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by Administrator on 2017/4/19.
  */
object TestConsole {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("KafkaSparkMySQL").master("local[8]")
      .getOrCreate()

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
    var streamingSelectDF =
      streamingInputDF
        .select(get_json_object(($"value").cast("string"), "$.zone_id").alias("zone_id"))
        .groupBy($"zone_id")
        .count()
    //.as[(String, Long)]
    val query = streamingSelectDF.writeStream.format("console").outputMode("complete").start()
    query.awaitTermination()
  }
}
