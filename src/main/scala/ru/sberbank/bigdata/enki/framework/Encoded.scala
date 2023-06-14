package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

//TODO: возможно следует совместить с HasEncoder и предоставить реализацию по-умолчанию.
// Обоснование:
// - Cейчас реализация содержится в TableStore, но работа с энкодерами не спецефична для конкретных источников данных (таблиц).
// - HasEncoder используется только в методах encode/decode
// Минусы:
// - SourceTableConfiguration не использует encoder, ему эти методы нужны только для трансформации на уровне датасета.
trait Encoded[T] {
  def encode(data: Dataset[T])(implicit session: SparkSession): DataFrame

  def decode(dataFrame: DataFrame)(implicit session: SparkSession): Dataset[T]
}
