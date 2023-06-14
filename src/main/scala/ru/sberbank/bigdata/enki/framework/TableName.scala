package ru.sberbank.bigdata.enki.framework

trait TableName extends Name {
  def schema: String

  def path: Option[String] = None

  def qualifiedName: String = if (schema != "") s"$schema.$name" else name
}
