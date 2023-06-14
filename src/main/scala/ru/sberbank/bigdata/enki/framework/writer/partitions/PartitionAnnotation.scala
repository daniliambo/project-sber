package ru.sberbank.bigdata.enki.framework.writer.partitions

import scala.annotation.StaticAnnotation

trait PartitionAnnotation extends StaticAnnotation {
  def order: Int
}
