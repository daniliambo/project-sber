package ru.sberbank.bigdata.enki.syntax

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DecimalType, StructField}
import org.apache.spark.sql.{Column, Dataset}

import scala.reflect.runtime.universe._

package object dataset {

  implicit class DatasetExtensions[T: TypeTag](dataset: Dataset[T]) {

    def decimalTo3818: Dataset[T] = {
      implicit val encoder = ExpressionEncoder[T]
      val schema           = dataset.schema
      dataset.select(schema.map(ru.sberbank.bigdata.enki.syntax.dataset.decimalTo3818): _*).as[T]
    }
  }

  /** Convert all Decimal datatype to Decimal(38, 18) to match scala.BigDecimal in case class */
  val decimalTo3818: StructField => Column = field =>
    field.dataType match {
      case decimalType: DecimalType if decimalType != DecimalType(38, 18) =>
        col(field.name).cast(DecimalType(38, 18)).as(field.name)
      case _ =>
        col(field.name)
    }

}
