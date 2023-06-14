package ru.sberbank.bigdata.enki.framework

import enumeratum._

import scala.collection.immutable

sealed abstract class AppendExistingAction(name: String) extends EnumEntry

object AppendExistingAction extends Enum[AppendExistingAction] {
  val values: immutable.IndexedSeq[AppendExistingAction] = findValues

  case object Overwrite               extends AppendExistingAction("Overwrite")
  case object Skip                    extends AppendExistingAction("Skip")
  case object ThrowException          extends AppendExistingAction("ThrowException")
  private[enki] case object DummyMode extends AppendExistingAction("DummyMode")
}
