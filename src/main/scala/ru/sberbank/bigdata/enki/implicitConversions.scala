package ru.sberbank.bigdata.enki

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object implicitConversions {
  private lazy val f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  implicit class DateTimeFormatterExtensions(formatter: DateTimeFormatter) {

    def parseTimestamp(value: String): Timestamp =
      Timestamp.valueOf(LocalDateTime.parse(value, formatter))
  }

  implicit def dateStrToMillis(date: String): Long =
    f.parseTimestamp(date).getTime

  implicit def dateStrToTimeStamp(date: String): Timestamp =
    new Timestamp(f.parseTimestamp(date).getTime)

  implicit def dateStrOptToTimeStamp(date: String): Option[Timestamp] =
    Some(new Timestamp(f.parseTimestamp(date).getTime))

  implicit def intToLongOption(x: Int): Option[Long] = Option(x)

  //  = Option(x) results in:
  //  Suspicious application of an implicit view (math.this.BigDecimal.int2bigDecimal) in the argument to Option.apply.
  implicit def intToBigDecimalOption(x: Int): Option[BigDecimal] = Option(BigDecimal(x))

  implicit def intToDoubleOption(x: Int): Option[Double] = Option(x)

  implicit def sbtToBigDecimal(x: String): BigDecimal = BigDecimal(x)

  implicit def pathToOptionString(x: scala.reflect.io.Path): Option[String] = Option(x.toString)

}
