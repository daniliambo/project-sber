package ru.sberbank.bigdata.enki.framework

final case class SourceTableConfiguration(
  enableTypeConversion: Boolean        = false,
  columns: Option[Map[String, String]] = None
) {
  def mappedColumns: Map[String, String] = columns.getOrElse(Map.empty[String, String])
}
