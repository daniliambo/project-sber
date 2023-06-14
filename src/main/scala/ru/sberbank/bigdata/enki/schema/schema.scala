package ru.sberbank.bigdata.enki

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession}
import ru.sberbank.bigdata.enki.annotation._
import ru.sberbank.bigdata.enki.metadata.MetadataAnnotation

import scala.reflect.runtime.universe._

/** Schema extraction and transformation including annotations-driven metadata. */
package object schema {

  def columnName(col: Column): String =
    col.expr match {
      case unresolvedAttribute: UnresolvedAttribute => unresolvedAttribute.nameParts.last
      case named: NamedExpression                   => named.name
      case other                                    => throw new Exception(s"Column $other does not have name.")
    }

  /** Compare two schema and return difference */
  def diff[T1: TypeTag, T2: TypeTag](implicit sparkSession: SparkSession): StructTypeDiff =
    diff(bareSchemaOf[T1], bareSchemaOf[T2])

  /** Construct schema for the type tag. Will not apply any metadata annotations which allows substitution. */
  def bareSchemaOf[T: TypeTag](implicit spark: SparkSession): StructType = {
    implicit val encoder: ExpressionEncoder[T] = ExpressionEncoder()

    spark.emptyDataset[T].schema
  }

  def schemaWithMetadataFromAnnotations[T: TypeTag](implicit spark: SparkSession): StructType = {
    implicit def encoder: Encoder[T] = ExpressionEncoder[T]

    withMetadataFromAnnotations(spark.emptyDataset[T]).schema
  }

  /** Extract metadata from annotations, apply substitutions and append it to dataset columns. */
  def withMetadataFromAnnotations[T: TypeTag](dataset: Dataset[T]): Dataset[T] = {
    implicit def encoder: Encoder[T] = ExpressionEncoder[T]()

    val klass = symbolOf[T].asClass

    val annotationMap = klass.primaryConstructor.typeSignature.paramLists.head.map { symbol =>
      symbol.name.toString -> symbol.annotations
        .filter(_.tree.tpe <:< typeOf[MetadataAnnotation])
        .map(instantiate(_).asInstanceOf[MetadataAnnotation])
    }.filter(_._2.nonEmpty).toMap

    val schema = dataset.schema
    val columns = schema.map { field =>
      annotationMap.get(field.name) match {
        case None => dataset(field.name)
        case Some(annotations) =>
          val metadata = new MetadataBuilder().withMetadata(field.metadata)
          annotations.foreach(annotation => metadata.putString(annotation.key, annotation.value))
          dataset(field.name).as(field.name, metadata.build())
      }
    }

    dataset.select(columns: _*).as[T]
  }

  /** Merge columns of two schema. Types of matching columns should match. */
  def merge(lt: StructType, rt: StructType, extend: Boolean): StructType = {
    val mergedFields = diff(lt, rt).fields.map {
      case Matched(f)   => f
      case LeftOnly(f)  => f.copy(nullable = true)
      case RightOnly(f) => f.copy(nullable = true)
      case Differ(lf, rf) =>
        if (lf.dataType == rf.dataType) {
          StructField(
            lf.name,
            lf.dataType,
            if (extend) lf.nullable || rf.nullable else lf.nullable && rf.nullable,
            new MetadataBuilder().withMetadata(lf.metadata).withMetadata(rf.metadata).build()
          )
        } else {
          throw new Exception(
            s"Field ${lf.name} types differ: ${lf.dataType} in the left dataset but ${rf.dataType} in the right dataset."
          )
        }
    }
    StructType(mergedFields)
  }

  /** Compare two schema and return difference */
  def diff(lt: StructType, rt: StructType): StructTypeDiff =
    StructTypeDiff(
      mergeByKey(
        lt.fields,
        rt.fields,
        (f: StructField) => f.name.toLowerCase,
        (f: StructField) => f.name.toLowerCase,
        LeftOnly,
        RightOnly,
        (l: StructField, r: StructField) =>
          if (l.dataType == r.dataType && l.nullable == r.nullable) Matched(l) else Differ(l, r)
      ).toArray
    )

  private def mergeByKey[X, Y, K, R](
    xs: Seq[X],
    ys: Seq[Y],
    kx: X => K,
    ky: Y => K,
    rx: X => R,
    ry: Y => R,
    rxy: (X, Y) => R
  ): Seq[R] = {
    val xmap   = xs.map(x => (kx(x), x)).toMap
    val ymap   = ys.map(y => (ky(y), y)).toMap
    val xkeys  = xmap.keySet.diff(ymap.keySet).toSeq
    val ykeys  = ymap.keySet.diff(xmap.keySet).toSeq
    val xykeys = xmap.keySet.intersect(ymap.keySet).toSeq
    xkeys.map(k => rx(xmap(k))) ++ ykeys.map(k => ry(ymap(k))) ++ xykeys.map(k => rxy(xmap(k), ymap(k)))
  }
}
