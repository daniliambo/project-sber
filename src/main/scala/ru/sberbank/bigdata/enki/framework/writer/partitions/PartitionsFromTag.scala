package ru.sberbank.bigdata.enki.framework.writer.partitions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import ru.sberbank.bigdata.enki.framework.HasTag

trait PartitionsFromTag[T] extends Partitioned[T] {
  self: HasTag[T] =>

  override private[enki] def partitionColumns: Seq[Column] = {
    val partitionColumns = PartitionsFromAnnotations[T](tag)
    partitionColumns.map(col)
  }
}
