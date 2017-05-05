package com.adups
import org.joda.time.LocalDate
object Utility {

  implicit class LocalDateCompare(val date: LocalDate) extends Ordered[LocalDateCompare] {
    def compare(that: LocalDateCompare) = this.date compareTo that.date
  }

}
