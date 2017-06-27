/**
  * Created by Administrator on 2017/6/19.
  */
package com.adups
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.{LongType,IntegerType, StringType, StructField, StructType}
object LogAdvSyncer extends SYNCProcess(QueueConfig.logAdvConf){
  implicit def KKHDFS[T](dataFrame: Dataset[T]):KKHDFS[T] = new KKHDFS[T](dataFrame){
    val longFields = List[String]()
    val intFields = List[String]()
    val stringFields =  List("proversion", "reportTag", "mac", "channel",
      "ad_type", "android_id", "provider", "model",
      "gaid", "os_version", "event", "imei", "mid",
      "packageName", "brand", "oper", "location_id",
      "modver", "language", "resolution", "sdk_version",
      "apn", "app_id", "ad_id", "time", "server_time","platform", "vendor", "value",
      "data_source", "intever", "click_effect","description","zone_id","uuid")
    val allFieldNames:List[String]= longFields ::: stringFields ::: intFields    //列举了所有字段，若有增删必须修改
    val jsonName:String="logadv"

    val timeFieldName = "server_time"   //指定用来表示时间的字段

    val schema = StructType(stringFields.map(fieldName => StructField(fieldName,StringType,true)) :::
      longFields.map(fieldName => StructField(fieldName,LongType,true)) :::
      intFields.map(fieldName => StructField(fieldName,IntegerType,true))    //提取json时必须指定schema
    )

  }

}