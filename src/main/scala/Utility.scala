package com.adups

import org.apache.spark.sql.{SparkSession, Dataset}
import org.joda.time.LocalDate
object Utility {

  implicit class LocalDateCompare(val date: LocalDate) extends Ordered[LocalDateCompare] {
    def compare(that: LocalDateCompare) = this.date compareTo that.date
  }
  implicit val spark: SparkSession = SparkSession
    .builder
    .appName("KafkaSparkMySQL")
    .getOrCreate()

}
