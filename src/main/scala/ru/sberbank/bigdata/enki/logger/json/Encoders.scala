package ru.sberbank.bigdata.enki.logger.json

import java.sql.Timestamp
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

import io.circe.Decoder.Result
import io.circe.{Decoder, Encoder, HCursor, Json}

object Encoders {

  private val timeFormatter: DateTimeFormatter = (new DateTimeFormatterBuilder)
    .appendPattern("yyyy-MM-dd HH:mm:ss")
    .appendFraction(ChronoField.MILLI_OF_SECOND, 0, 6, true)
    .toFormatter

  private def dateStrToTimeStamp(date: String): Timestamp = new Timestamp(parseTimestamp(date).getTime)

  private[json] def parseTimestamp(value: String): Timestamp =
    Timestamp.valueOf(LocalDateTime.parse(value, timeFormatter))

  implicit val timestampFormat: Encoder[Timestamp] with Decoder[Timestamp] =
    new Encoder[Timestamp] with Decoder[Timestamp] {
      override def apply(a: Timestamp): Json = Encoder.encodeString.apply(a.toString)

      override def apply(c: HCursor): Result[Timestamp] = Decoder.decodeString.map(dateStrToTimeStamp).apply(c)
    }

}
