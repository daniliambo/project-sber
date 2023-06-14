package ru.sberbank.bigdata.enki.spark2scala.exceptions

sealed class Spark2ScalaException(message: String, cause: Option[Throwable]) extends Exception(message, cause.orNull)

final case class UnsupportedConversion(cause: Option[Throwable])
    extends Spark2ScalaException(
      "Unsupported conversion! Please, consider to use ADTConverter::buildScalaClasses",
      cause
    )
