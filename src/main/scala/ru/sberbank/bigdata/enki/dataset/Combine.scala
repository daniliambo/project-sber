package ru.sberbank.bigdata.enki.dataset

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import ru.sberbank.bigdata.enki.schema._

import scala.reflect.runtime.universe._

object Combine {

  /** Combine rows by id populating fields with first not-null value.
    */
  def combine(sources: Seq[Dataset[_]], targetSchema: StructType, ordered: Boolean, keys: String*): DataFrame = {
    val keySet         = keys.map(_.toLowerCase).toSet
    val combinedSchema = sources.map(_.schema).foldLeft(targetSchema)(merge(_, _, extend = true))
    val combinedSource = sources.zipWithIndex.map { case (source, order) =>
      extend(source, combinedSchema).withColumn("__ord", lit(order))
    }.reduce(_.union(_))

    import combinedSource.sparkSession.implicits._

    val (keyFields, valueFields) = targetSchema.fields.partition(f => keySet.contains(f.name.toLowerCase))

    val keyColumns = keyFields.map(f => combinedSource(f.name))
    if (ordered) {
      val keyWindow = partitionBy(keyColumns: _*).orderBy("__ord")
      val valueColumns = valueFields.map { f =>
        first(combinedSource(f.name), ignoreNulls = true)
          .over(keyWindow.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
          .as(f.name, f.metadata)
      }
      combinedSource
        .select(keyColumns ++ valueColumns :+ row_number().over(keyWindow).as("__rn"): _*)
        .where($"__rn" === 1)
        .drop("__rn")
    } else {
      // Есть гипотеза, что группировка работает быстрее оконной функции, поскольку частично может
      // выполняться на маппере. Но её следует проверить.
      val valueColumns = valueFields.map { f =>
        first(combinedSource(f.name), ignoreNulls = true).as(f.name, f.metadata)
      }
      combinedSource.groupBy(keyColumns: _*).agg(valueColumns.head, valueColumns.tail: _*)
    }
  }

  implicit class CombineDatasetExtensions(sources: Seq[Dataset[_]]) {

    def combine[T: Encoder: TypeTag](ordered: Boolean, keys: String*)(implicit session: SparkSession): Dataset[T] = {
      val targetSchema = bareSchemaOf[T]
      ru.sberbank.bigdata.enki.dataset.Combine.combine(sources, targetSchema, ordered, keys: _*).as[T]
    }
  }
}
