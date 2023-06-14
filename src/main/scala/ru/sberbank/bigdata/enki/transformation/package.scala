package ru.sberbank.bigdata.enki

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import ru.sberbank.bigdata.enki.schema._

import scala.reflect.runtime.universe._

package object transformation {

  def convert[T2: TypeTag](src: Dataset[_], converters: TypeConverter*)(
    implicit sparkSession: SparkSession
  ): Dataset[T2] = {
    implicit val encoder = ExpressionEncoder[T2]
    convert(src, schema.bareSchemaOf[T2], converters: _*).as[T2]
  }

  def convert(src: Dataset[_], dstSchema: StructType, converters: TypeConverter*): DataFrame = {
    import src.sparkSession.implicits._
    convertStatus(src, dstSchema, converters: _*).select(dstSchema.map { c =>
      $"dst_${c.name}".as(c.name)
    }: _*)
  }

  /** Return source values, casted values, cast result per column and cast summary per row
    */
  def convertStatus(src: Dataset[_], dstSchema: StructType, converters: TypeConverter*): DataFrame = {
    import TransformationStatus._

    val srcSchema = src.schema

    def statusKey(from: String, to: String) = concat_ws("->", lit(from), lit(to))

    val (dataCols, statusCols) = diff(srcSchema, dstSchema).fields.foldLeft((List.empty[Column], List.empty[Column])) {
      (acc, diff) =>
        diff match {
          case Matched(f) =>
            (
              List(src(f.name).as(s"src_${f.name}"), src(f.name).as(s"dst_${f.name}")) ++ acc._1,
              List(statusKey(f.name, f.name), lit(Success)) ++ acc._2
            )
          case LeftOnly(f) =>
            (
              src(f.name).as(s"src_${f.name}") :: acc._1,
              List(statusKey(f.name, null), lit(Skipped)) ++ acc._2
            )
          case RightOnly(f) =>
            (
              lit(null).cast(f.dataType).as(s"dst_${f.name}") :: acc._1,
              List(statusKey(null, f.name), lit(Default)) ++ acc._2
            )
          case Differ(fromField, toField) =>
            val convert = converters.flatMap {
              case TypeConverter(from, to, f) if fromField.dataType == from && toField.dataType == to => Some(f)
              case _                                                                                  => None
            }.headOption.getOrElse { c: Column =>
              c.cast(toField.dataType)
            }

            val srcExpr = src(fromField.name)
            val dstExpr = convert(src(fromField.name))

            def casted(src: Column, dst: Column): Column = dst.isNotNull || src.isNull

            (
              List(srcExpr.as(s"src_${fromField.name}"), dstExpr.as(s"dst_${toField.name}")) ++ acc._1,
              List(statusKey(fromField.name, toField.name),
                   when(casted(srcExpr, dstExpr), Success).otherwise(Error)
              ) ++ acc._2
            )
        }
    }

    src.select(map(statusCols: _*).as("convert_status") :: dataCols: _*)
  }

  def srcFromStatus[T: TypeTag](status: Dataset[_])(implicit sparkSession: SparkSession): Dataset[T] =
    fromStatus(status, "src")

  def dstFromStatus[T: TypeTag](status: Dataset[_])(implicit sparkSession: SparkSession): Dataset[T] =
    fromStatus(status, "dst")

  private def fromStatus[T: TypeTag](status: Dataset[_], colPrefix: String)(
    implicit sparkSession: SparkSession
  ): Dataset[T] = {
    implicit val encoder = ExpressionEncoder[T]
    status.select(bareSchemaOf[T].map(f => status(s"${colPrefix}_${f.name}").as(f.name)): _*).as[T]
  }
}
