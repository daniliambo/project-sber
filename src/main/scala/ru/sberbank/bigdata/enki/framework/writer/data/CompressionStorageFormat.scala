package ru.sberbank.bigdata.enki.framework.writer.data

import enumeratum._
import pureconfig.error.ConfigReaderFailures
import pureconfig.{ConfigCursor, ConfigReader}
import ru.sberbank.bigdata.enki.framework.extensions.EnumExtensions
import ru.sberbank.bigdata.enki.framework.writer.data.CompressionCodec.{Gzip, None, Snappy, Zlib}
import ru.sberbank.bigdata.enki.framework.writer.data.DataStorageFormat.{Csv, Orc, Parquet}

import scala.collection.immutable

sealed abstract class CompressionStorageFormat(val dataFormat: DataStorageFormat, val compression: CompressionCodec)
    extends EnumEntry

object CompressionStorageFormat extends Enum[CompressionStorageFormat] with CirceEnum[CompressionStorageFormat] {

  implicit val values: immutable.IndexedSeq[CompressionStorageFormat] = findValues

  case object ParquetSnappy extends CompressionStorageFormat(Parquet, Snappy)
  case object ParquetGzip   extends CompressionStorageFormat(Parquet, Gzip)
  case object OrcSnappy     extends CompressionStorageFormat(Orc, Snappy)
  case object OrcZlib       extends CompressionStorageFormat(Orc, Zlib)
  case object CsvGzip       extends CompressionStorageFormat(Csv, Gzip)
  case object CsvNone       extends CompressionStorageFormat(Csv, None)

  implicit class CompressStoreFormatExtensions(value: String)
      extends EnumExtensions[CompressionStorageFormat](value: String)

  /* Used for implicit conversion from string to CtlAction by pureconfig library */
  implicit object CompressStoreFormatReader extends ConfigReader[CompressionStorageFormat] {

    override def from(cur: ConfigCursor): Either[ConfigReaderFailures, CompressionStorageFormat] =
      cur.asString.fold(x => Left(x), y => Right(y.asInstanceofEnum))
  }

}
