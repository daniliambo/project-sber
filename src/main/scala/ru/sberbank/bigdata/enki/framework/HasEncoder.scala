package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.types.StructType

trait HasEncoder[T] {
  def encoder(schema: StructType): Encoder[T]
}
