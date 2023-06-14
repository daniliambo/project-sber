package ru.sberbank.bigdata.enki.datagen

import java.sql.Timestamp

import cats.effect.{ContextShift, IO}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ru.sberbank.bigdata.enki.framework.{Table, WorkflowTask}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._

object SyntheticDataGenerator {
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def sampleDataset(targetView: WorkflowTask[_], rowCount: Int)(implicit spark: SparkSession): Dataset[Row] = {
    val schema  = targetView.asInstanceOf[Table[_]].getEmpty().schema
    val rowType = targetView.asInstanceOf[Table[_]].getTypeParameter

    sampleDataset(schema, rowType, rowCount)
  }

  def sampleDataset(targetSchema: StructType, targetRowType: Type, rowCount: Int)(
    implicit spark: SparkSession
  ): Dataset[Row] = {
    val colOrder = targetSchema.fieldNames.map(_.toString.toLowerCase).toVector

    val dataForRows: Seq[Map[String, Any]] =
      for (value <- 1 to rowCount)
        yield targetRowType.members
          .filter(!_.isMethod)
          .map { field =>
            field.name.toString.toLowerCase.trim -> valueOfType(value, field.typeSignature)
          }
          .toMap

    val rows = dataForRows.map { dataRow =>
      Row.fromSeq(colOrder.map(dataRow))
    }

    spark.createDataFrame(rows, targetSchema)
  }

  private[datagen] def valueOfType(value: Int, valueType: Type): Any = {
    val resValue =
      valueType match {
        case t if t =:= typeOf[Int] || t =:= typeOf[Option[Int]]               => value
        case t if t =:= typeOf[BigInt] || t =:= typeOf[Option[BigInt]]         => BigInt(value)
        case t if t =:= typeOf[Long] || t =:= typeOf[Option[Long]]             => value.toLong
        case t if t =:= typeOf[BigDecimal] || t =:= typeOf[Option[BigDecimal]] => BigDecimal(value)
        case t if t =:= typeOf[String] || t =:= typeOf[Option[String]]         => value.toString
        case t if t =:= typeOf[Timestamp] || t =:= typeOf[Option[Timestamp]] =>
          new Timestamp(defaultTs + value * nanoseconds)
        case t if t =:= typeOf[Double] || t =:= typeOf[Option[Double]]   => value.toDouble
        case t if t =:= typeOf[Boolean] || t =:= typeOf[Option[Boolean]] => true
        case _                                                           => throw new IllegalArgumentException(s"Provided valueType $valueType not yet supported")
      }

    resValue.asInstanceOf[Any]
  }

}
