package ru.sberbank.bigdata.enki.syntax

import java.sql.Timestamp
import java.time.temporal.ChronoUnit.DAYS
import java.time._

package object timestamp {

  implicit class TimestampExtensions(timestamp: Timestamp) {
    def atNoon: Timestamp = adjust(time => LocalDateTime.of(time.toLocalDate, LocalTime.NOON))

    def atStartOfDay: Timestamp = adjust(_.toLocalDate.atStartOfDay())

    def atStartOfMonth: Timestamp = adjust(_.toLocalDate.withDayOfMonth(1).atStartOfDay())

    def atStartofYear: Timestamp = adjust(_.toLocalDate.withMonth(1).withDayOfMonth(1).atStartOfDay())

    def atEndofYear: Timestamp = adjust(_.toLocalDate.withMonth(12).withDayOfMonth(31).atStartOfDay())

    def daysBetween(other: Timestamp): Long =
      DAYS.between(timestamp.toLocalDateTime, other.toLocalDateTime)

    def minusNanos(nanos: Long): Timestamp = adjust(_.minusNanos(nanos))

    def minusSeconds(seconds: Long): Timestamp = adjust(_.minusSeconds(seconds))

    def minusMinutes(minutes: Long): Timestamp = adjust(_.minusMinutes(minutes))

    def minusHours(hours: Long): Timestamp = adjust(_.minusHours(hours))

    def minusDays(days: Long): Timestamp = adjust(_.minusDays(days))

    def minusMonths(months: Long): Timestamp = adjust(_.minusMonths(months))

    def minusYears(years: Long): Timestamp = adjust(_.minusYears(years))

    def isStartOfDay: Boolean = project(_.toLocalTime == LocalTime.MIDNIGHT)

    def plusNanos(nanos: Long): Timestamp = adjust(_.plusNanos(nanos))

    def plusSeconds(seconds: Long): Timestamp = adjust(_.plusSeconds(seconds))

    def plusMinutes(minutes: Long): Timestamp = adjust(_.plusMinutes(minutes))

    def plusHours(hours: Long): Timestamp = adjust(_.plusHours(hours))

    def plusDays(days: Long): Timestamp = adjust(_.plusDays(days))

    def plusMonth(months: Long): Timestamp = adjust(_.plusMonths(months))

    def plusYears(years: Long): Timestamp = adjust(_.plusYears(years))

    def minusDays(days: Int): Timestamp = adjust(_.minusDays(days))

    def minusMonths(months: Int): Timestamp = adjust(_.minusMonths(months))

    def minusYears(years: Int): Timestamp = adjust(_.minusYears(years))

    private def adjust(f: LocalDateTime => LocalDateTime): Timestamp = Timestamp.valueOf(f(timestamp.toLocalDateTime))

    def plusDays(days: Int): Timestamp = adjust(_.plusDays(days))

    def plusMonth(months: Int): Timestamp = adjust(_.plusMonths(months))

    def plusYears(years: Int): Timestamp = adjust(_.plusYears(years.toLong))

    def year: Int = project(_.getYear)

    def quarterOfYear: Int = project(date => (date.getMonthValue - 1) / 3 + 1)

    private def project[T](f: LocalDateTime => T): T = f(timestamp.toLocalDateTime)

    def monthOfYear: Int = project(date => date.getMonthValue)

    def toLocalDate: LocalDate =
      Instant.ofEpochMilli(timestamp.getTime).atZone(ZoneId.of("Europe/Moscow")).toLocalDate

    def toLocalDateTime: LocalDateTime =
      Instant.ofEpochMilli(timestamp.getTime).atZone(ZoneId.of("Europe/Moscow")).toLocalDateTime
  }

}
