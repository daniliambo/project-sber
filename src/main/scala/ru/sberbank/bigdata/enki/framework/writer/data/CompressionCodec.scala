package ru.sberbank.bigdata.enki.framework.writer.data

import enumeratum._
import ru.sberbank.bigdata.enki.framework.extensions.EnumExtensions

import scala.collection.immutable

sealed abstract private[writer] class CompressionCodec(extension: String) extends EnumEntry {
  override def toString: String = extension
}

object CompressionCodec extends Enum[CompressionCodec] with CirceEnum[CompressionCodec] {

  implicit val values: immutable.IndexedSeq[CompressionCodec] = findValues

  case object Snappy extends CompressionCodec("snappy")
  case object Gzip   extends CompressionCodec("gzip")
  case object LZO    extends CompressionCodec("lzo")
  case object Zlib   extends CompressionCodec("zlib")
  case object None   extends CompressionCodec("none")

  implicit class CompressionCodecExtensions(value: String) extends EnumExtensions[CompressionCodec](value: String)

}
