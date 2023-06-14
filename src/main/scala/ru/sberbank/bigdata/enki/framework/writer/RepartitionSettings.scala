package ru.sberbank.bigdata.enki.framework.writer

final case class RepartitionSettings(
  partitionColumns: Seq[String],
  numPartitions: Option[Int]
)
