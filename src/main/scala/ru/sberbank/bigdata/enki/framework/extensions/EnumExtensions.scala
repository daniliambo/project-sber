package ru.sberbank.bigdata.enki.framework.extensions

import enumeratum._

import scala.collection.immutable

/** Adds asInstanceofEnum for easy conversion from String to Enum from Enumeratum library
  * Needs instance of class extending EnumExtensions in scope to be used
  */
abstract private[enki] class EnumExtensions[T <: EnumEntry](value: String)(implicit values: immutable.IndexedSeq[T]) {

  def asInstanceofEnum: T =
    values.find(_.toString.equalsIgnoreCase(value)) match {
      case Some(enumEntry) => enumEntry
      case _               => throw new Exception(s"No such Enum Value $value, possible variants: ${values.mkString(",")}")
    }

}
