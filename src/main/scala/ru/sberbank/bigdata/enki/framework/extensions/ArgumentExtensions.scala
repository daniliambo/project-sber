package ru.sberbank.bigdata.enki.framework.extensions

import cats.data.ValidatedNel
import cats.implicits._
import com.monovore.decline.Argument

abstract private[enki] class ArgumentExtensions[T](val defaultMetavar: String) extends Argument[T] {

  override def read(string: String): ValidatedNel[String, T] =
    Either.catchNonFatal(parseUnsafe(string)).leftMap(_.getMessage).toValidatedNel

  protected def parseUnsafe: String => T

}
