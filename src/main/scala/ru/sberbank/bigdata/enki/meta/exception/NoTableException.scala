package ru.sberbank.bigdata.enki.meta.exception

final private[meta] case class NoTableException(detailedMessage: String) extends Exception(detailedMessage)
