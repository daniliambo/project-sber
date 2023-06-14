package ru.sberbank.bigdata.enki.framework

import scala.reflect.runtime.universe._

trait HasTag[T] {
  // can't make it implicit due to "super constructor cannot be passed
  // a self reference unless parameter is declared by-name" error.
  def tag: TypeTag[T]
}
