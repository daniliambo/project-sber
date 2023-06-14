package ru.sberbank.bigdata.enki.keys

import org.apache.spark.sql.{Column, Dataset}
import ru.sberbank.bigdata.enki.schema._

trait HasKey[T] {
  //TODO: убрать этот метод
  def key(dataset: Dataset[T]): Seq[Column]

  def keyColNames(dataset: Dataset[T]): Seq[String] = key(dataset).map(columnName)
}
