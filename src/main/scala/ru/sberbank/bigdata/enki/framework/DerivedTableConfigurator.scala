package ru.sberbank.bigdata.enki.framework

//TODO: Тут логичнее использовать self-types а не extension. Конфигуратору незачем быть именем таблицы.
trait DerivedTableConfigurator extends TableName {
  def configuration: DerivedTableConfiguration

  override def schema: String = configuration.schema

  override def name: String = configuration.table

  override def path: Option[String] = configuration.pathOpt
}
