package ru.sberbank.bigdata.enki.logger

import enumeratum._

import scala.collection.immutable

sealed abstract class DataExistingOperation(name: String) extends EnumEntry

object DataExistingOperation extends Enum[DataExistingOperation] {
  val values: immutable.IndexedSeq[DataExistingOperation] = findValues

  case object Read extends DataExistingOperation("Read")

  case object Write extends DataExistingOperation("Write")

  case object Delete extends DataExistingOperation("Delete")

}
