package ru.sberbank.bigdata.enki.framework.writer.data

import ru.sberbank.bigdata.enki.framework.writer.data.CompressionStorageFormat.ParquetSnappy

trait CompressedStored {

  val csvOptions: Map[String, String] = Map(
    "format"          -> "csv",
    "timestampFormat" -> "yyyy-MM-dd HH:mm:ss",
    "header"          -> "true",
    "delimiter"       -> ";"
  ) // TODO: Better to parametrize these as well

  def codecDataFormat: CompressionStorageFormat = ParquetSnappy
}
