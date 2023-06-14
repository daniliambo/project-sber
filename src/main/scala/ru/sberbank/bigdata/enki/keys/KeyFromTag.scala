package ru.sberbank.bigdata.enki.keys

import org.apache.spark.sql.{Column, Dataset}
import ru.sberbank.bigdata.enki.framework.HasTag

trait KeyFromTag[T] extends HasKey[T] {
  self: HasTag[T] =>

  override def key(dataset: Dataset[T]): Seq[Column] = {
    val keyColumns = primaryKeyFromAnnotation[T](tag)
    keyColumns.map(dataset(_))
  }
}
