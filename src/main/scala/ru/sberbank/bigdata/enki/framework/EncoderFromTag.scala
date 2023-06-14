package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoder, Row}

import scala.reflect.runtime.universe._

trait EncoderFromTag[T] extends HasEncoder[T] {
  self: HasTag[T] =>

  override def encoder(schema: StructType): Encoder[T] =
    EncoderFromTag[T](schema)(tag)
}

object EncoderFromTag {

  def apply[T: TypeTag](schema: StructType): Encoder[T] =
    if (typeOf[T] == typeOf[Row]) {
      RowEncoder(schema).asInstanceOf[Encoder[T]]
    } else {
      ExpressionEncoder[T]()
    }
}
