package ru.sberbank.bigdata.enki.sql

import java.sql.Timestamp

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

object TypedDatasetSyntax extends OptionColumnRelation {

  implicit class DataframeSyntax(dataframe: DataFrame) {
    def typed[T: Encoder]: TypedDataset[T] = TypedDataset(dataframe.as[T], None)

    def aliased[T: Encoder](alias: String): TypedDataset[T] = TypedDataset(dataframe.as[T].as(alias), Some(alias))
  }

  implicit class DatasetSyntax[T: Encoder](dataset: Dataset[T]) {
    def typed: TypedDataset[T] = TypedDataset(dataset, None)

    def aliased(alias: String): TypedDataset[T] = TypedDataset(dataset.as(alias), Some(alias))
  }

  implicit val timestampEncoder: ExpressionEncoder[Timestamp] = ExpressionEncoder[Timestamp]

  implicit val bigDecimalEncoder: ExpressionEncoder[BigDecimal] = ExpressionEncoder[BigDecimal]

}
