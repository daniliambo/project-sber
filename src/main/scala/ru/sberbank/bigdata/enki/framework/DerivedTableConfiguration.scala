package ru.sberbank.bigdata.enki.framework

final case class DerivedTableConfiguration(
  schema: String,
  table: String,
  path: Option[String]     = None,
  histPath: Option[String] = None
) {

  def pathOpt: Option[String] = path

  def histPathOpt: Option[String] = histPath
}
