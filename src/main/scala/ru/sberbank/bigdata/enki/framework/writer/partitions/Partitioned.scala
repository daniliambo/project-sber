package ru.sberbank.bigdata.enki.framework.writer.partitions

import org.apache.spark.sql.{Column, SparkSession}
import ru.sberbank.bigdata.enki.schema._

trait Partitioned[T] {

  private[enki] def partitionColumns: Seq[Column]

  def numPartitions: Option[Int] = None

  // Additional numPartitions parameter hides inner realisation of partitions calculation logic from user
  private[enki] def enkiNumPartitions(implicit spark: SparkSession): Option[Int] = numPartitions

  // Not a part of partition logic, yet it influences repartitionColNames
  def maxRecordsPerFile: Option[Long] = None

  def partitionColNames: Seq[String] = partitionColumns.map(columnName)

  def repartitionColNames: Seq[String] = if (maxRecordsPerFile.isDefined) Seq() else partitionColNames

}
