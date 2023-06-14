package ru.sberbank.bigdata.enki.business

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Encoder, SparkSession}
import ru.sberbank.bigdata.enki.sql.TypedDataset
import ru.sberbank.bigdata.enki.sql.TypedDatasetSyntax._

object Checker {

  def isCheckSumGood(innCol: Column, factors: Array[Int]): Column = {
    val sum = factors.indices.foldLeft(lit(0)) { (acc, i) =>
      acc + innCol.substr(i + 1, 1) * factors(i)
    }
    val controlNum = sum % 11 % 10

    when(innCol.substr(factors.length + 1, 1) === controlNum, true).otherwise(false)
  }

  def isInnValid(innCol: Column): Column = {
    val factor10    = Array(2, 4, 10, 3, 5, 9, 4, 6, 8)
    val factor12    = Array(7) ++ factor10
    val factor12Ext = Array(3) ++ factor12

    val checkInn10 = isCheckSumGood(innCol, factor10)
    val checkInn12 = when(isCheckSumGood(innCol, factor12), isCheckSumGood(innCol, factor12Ext)).otherwise(false)

    when(length(innCol) === 10, checkInn10)
      .when(length(innCol) === 12, checkInn12)
      .otherwise(false)
  }

  def getInvalidInn(df: DataFrame, innColName: String): DataFrame = {
    val inn = col(innColName)

    df.select(inn)
      .distinct
      .where(!isInnValid(inn))
  }

  def getInvalidInn[T: Encoder](df: TypedDataset[T], innColName: String)(
    implicit spark: SparkSession
  ): TypedDataset[T] = {
    val inn = col(innColName)

    df.select(inn)
      .distinct
      .where(!isInnValid(inn))
      .as[T]
      .typed
  }
}
