package com.adups
import java.util.TimeZone
import grizzled.slf4j.Logger
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import Utility._

/**
  * Created by Administrator on 2017/6/7.
  */
abstract class KKHDFS[T](val dataFrame: Dataset[T]) {
  def allFieldNames:List[String]
  def schema:StructType
  def jsonName:String
  def timeFieldName:String
  def extractJson() = {
    dataFrame.select(from_json(dataFrame("value").cast("string"),schema) as jsonName)
  }
  def allColumnsWithPrefix={
    allFieldNames.map(s => dataFrame(jsonName + "." + s))
  }
  def selectAllColumns={
    dataFrame.select(allColumnsWithPrefix:_*)
  }
  def allColumns={
    allFieldNames.map(s => dataFrame(s))
  }
  def addPt={
    val ptColumn = date_format(dataFrame(timeFieldName).cast("timestamp"),"yyyy-MM-dd") as "pt"
    val g =  allColumns:+ ptColumn
    dataFrame.select(g:_*)
  }
  def outputParquet(outputDir:String,checkpointLocation:String,triggerTime:String)={


    dataFrame.writeStream
      .format("parquet")
      .partitionBy("pt")
      .option("path", outputDir)
      .option("checkpointLocation", checkpointLocation)
      .trigger(ProcessingTime(triggerTime))
      .outputMode(OutputMode.Append)
      .start()
  }
  def outputConsole()={
    dataFrame.writeStream.format("console").outputMode(OutputMode.Append).start()
  }
}
abstract class SYNCProcess(val config:MySqlKafka)  {
  TimeZone.setDefault(TimeZone.getTimeZone("GMT+0")) //由于窗口函数是以UTC为计算单位的，而不是以当前时区为计算单位
  @transient lazy val logger = Logger[this.type]

  implicit def KKHDFS[T](dataFrame: Dataset[T]):KKHDFS[T]

  val MySqlKafka(url,user,pwd,bootstrap,topic)=config

  def createKafkaStream()(implicit spark:SparkSession) ={
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .option("maxPartitions", 10)
      .option("kafkaConsumer.pollTimeoutMs", 512)
      .option("failOnDataLoss", false).load()
  }
  def run(customTrigerTime:String,outputPath:String,checkpointLocation:String) {
    logger.info("Begin Running Spark Stream")
    while(true) {
      try {
        val query = createKafkaStream()  //注释与cp实时统计类似
          .extractJson
          .selectAllColumns
          .addPt
          .outputParquet(outputPath,checkpointLocation,customTrigerTime) //以parquet格式输出到hdfs
        query.awaitTermination()
      } catch {
        case ex:Throwable => logger.error("spark stream:" + ex.getMessage())
      }
    }
  }
  def run(){
    logger.info("Begin Running Spark Stream")
    while(true) {
      try {
        val query = createKafkaStream()
          .extractJson
          .selectAllColumns
          .addPt
          .outputConsole
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
          |Usage: com.adups.CPStatistical <间隔时间量> <间隔时间单位> <checkpointLocation>.
          |     <间隔时间量> 必须是整数
          |     <间隔时间单位> minutes,seconds,etc.
          |     <outputPath>
          |     <checkpointLocation>
          |
        """.
          stripMargin
      )
      System.exit(1)

    }
    if (args.length == 1 && args(0) == "console") run
    else if (args.length != 4) errorRemind()
    else{
      val Array(intervalNum, intervalUnit,outputPath, checkpointLocation) = args
      val timeUnitSet = Set("minutes", "seconds", "hours", "days")
      try {
        intervalNum.toInt
        val lowerIntevalUnit = intervalUnit.toLowerCase
        if (!timeUnitSet(lowerIntevalUnit))
          throw new Throwable("Error Unit")
        println(s"$intervalNum $intervalUnit")
        run(s"$intervalNum $intervalUnit",outputPath, checkpointLocation)


      } catch {
        case ex: Throwable => {
          logger.error("Parameters Error:" + ex)
          errorRemind()

        }
      }
    }

  }
}
