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

  //val checkpointLocation:String
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
      .option("startingOffsets", "latest") //此处指定从kafka读取位移位置，如果现场目录指定了读取位置，那么该参数将会忽略，如何设置参考https://spark.apache.org/docs/2.1.0/structured-streaming-kafka-integration.html
      .option("maxPartitions", 10)
      .option("kafkaConsumer.pollTimeoutMs", 512)
      .option("failOnDataLoss", false).load()
  }
  def run(customTrigerTime:String,checkpointLocation:String) {
    logger.info("Begin Running Spark Stream")
    while(true) {
      try {
        val query = createKafkaStream()  //创建流，订阅kafka信息
          .extractJson                   //提取kafka流的有效数据，并转换为dataframe的嵌套结构
          .selectUsedColumns             //提取需要用来统计的字段
          .castServerTime                //将时间字段转换为时间戳，以便后面的计算
          .stats                         //加上时间窗口 进行group by 统计
          .outputCPMidCount[U]           //强制转换为CPMidCount类型，这个类型也是本程序定义的
          .outputMysql(writer,checkpointLocation,customTrigerTime) //输出到mysql，同时也指定了现场保存目录和运行的间隔时间
        query.awaitTermination()
      } catch {
        case ex:Throwable => logger.error("spark stream:" + ex.getMessage())
      }
      Thread.sleep(2 * 60 * 1000)
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
          |     <checkpointLocation>  存放运行现场的hdfs目录
          |
        """.
            stripMargin
        )
        System.exit(1)

    }
    if (args.length != 3) errorRemind()
    val Array(intervalNum,intervalUnit,checkpointLocation)=args //分别将三个参数提取出来
    val timeUnitSet = Set("minutes","seconds","hours","days")
    try{
      intervalNum.toInt   //检查时间间隔是否是整数
      val lowerIntevalUnit = intervalUnit.toLowerCase
      if(! timeUnitSet(lowerIntevalUnit))  //检查时间间隔单位是否合法
        throw new Throwable("Error Unit")
      println(s"$intervalNum $intervalUnit")
      run(s"$intervalNum $intervalUnit",checkpointLocation) //注意 此处未检查hdfs目录是否合法，因此运行之前用hdfs dfs -ls 检查一下


    }catch{
      case ex:Throwable => {
        logger.error("Parameters Error:"+ ex)
        errorRemind()

      }
    }

  }
}