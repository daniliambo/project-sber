package ru.sberbank.bigdata.enki.framework.writer.data

import enumeratum._
import ru.sberbank.bigdata.enki.framework.extensions.EnumExtensions

import scala.collection.immutable

sealed abstract private[writer] class DataStorageFormat(format: String) extends EnumEntry {
  override def toString: String = format
}

object DataStorageFormat extends Enum[DataStorageFormat] with CirceEnum[DataStorageFormat] {

  implicit val values: immutable.IndexedSeq[DataStorageFormat] = findValues

  case object Parquet extends DataStorageFormat("parquet")
  case object Csv     extends DataStorageFormat("csv")
  case object Orc     extends DataStorageFormat("orc")

  implicit class CompressionCodecExtensions(value: String) extends EnumExtensions[DataStorageFormat](value: String)

}
