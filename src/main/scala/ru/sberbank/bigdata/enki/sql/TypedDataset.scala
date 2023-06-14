package ru.sberbank.bigdata.enki.sql

import org.apache.spark.api.java.function.{ForeachFunction, ForeachPartitionFunction}
import org.apache.spark.sql._
import cats.implicits._
import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import ru.sberbank.bigdata.enki.sql.TypedDatasetSyntax._
import ru.sberbank.bigdata.enki.sql.exceptions.ProjectionException

import scala.language.experimental.macros
import scala.reflect.runtime.universe._

final case class TypedDataset[T](dataset: Dataset[T], alias: Option[String]) {

  @transient val sparkSession: SparkSession = dataset.sparkSession

  def col[U: Encoder](colName: String): TypedColumn[T, U] = untypedCol(colName).as[U]

  private def untypedCol(colName: String): Column = alias match {
    case Some(a) => org.apache.spark.sql.functions.col(s"$a.$colName")
    case None    => dataset.col(colName)
  }

  def aliased(alias: String): TypedDataset[T] =
    if (this.alias.contains(alias)) {
      this
    } else {
      TypedDataset(dataset.as(alias), Some(alias))
    }

  def apply(colName: String): Column = untypedCol(colName)

  def apply[A, R](selector: T => A)(implicit relation: TypeRelation[A, R], encoder: Encoder[R]): TypedColumn[T, R] =
    macro ColumnMacros.fromFunction[T, A, R]

  private def rewrap[U](f: Dataset[T] => Dataset[U]): TypedDataset[U] = {
    val mappedDataset = f(dataset)
    TypedDataset(alias.map(a => mappedDataset.as(a)).getOrElse(mappedDataset), alias)
  }

  def count(): Long = dataset.count()

  def groupBy(cols: Column*): RelationalGroupedDataset = dataset.groupBy(cols: _*)

  @scala.annotation.varargs
  def groupBy(col1: String, cols: String*): RelationalGroupedDataset = dataset.groupBy(col1, cols: _*)

  def join(right: Dataset[_]): DataFrame = dataset.join(right)

  def join(right: TypedDataset[_]): DataFrame = dataset.join(right.dataset)

  def join(right: Dataset[_], usingColumn: String): DataFrame = dataset.join(right, usingColumn)

  def join(right: TypedDataset[_], usingColumn: String): DataFrame = dataset.join(right.dataset, usingColumn)

  def join(right: Dataset[_], usingColumns: Seq[String]): DataFrame = dataset.join(right, usingColumns)

  def join(right: TypedDataset[_], usingColumns: Seq[String]): DataFrame = dataset.join(right.dataset, usingColumns)

  def join(right: Dataset[_], usingColumns: Seq[String], joinType: String): DataFrame =
    dataset.join(right, usingColumns, joinType)

  def join(right: TypedDataset[_], usingColumns: Seq[String], joinType: String): DataFrame =
    dataset.join(right.dataset, usingColumns, joinType)

  def join(right: Dataset[_], joinExprs: Column): DataFrame = dataset.join(right, joinExprs)

  def join(right: TypedDataset[_], joinExprs: Column): DataFrame = dataset.join(right.dataset, joinExprs)

  def join(right: Dataset[_], joinExprs: Column, joinType: String): DataFrame = dataset.join(right, joinExprs, joinType)

  def join(right: TypedDataset[_], joinExprs: Column, joinType: String): DataFrame =
    dataset.join(right.dataset, joinExprs, joinType)

  def crossJoin(right: Dataset[_]): DataFrame = dataset.crossJoin(right)

  def crossJoin(right: TypedDataset[_]): DataFrame = dataset.crossJoin(right.dataset)

  @scala.annotation.varargs
  def sortWithinPartitions(sortCol: String, sortCols: String*): TypedDataset[T] = rewrap(
    _.sortWithinPartitions(sortCol, sortCols: _*)
  )

  @scala.annotation.varargs
  def sortWithinPartitions(sortExprs: Column*): TypedDataset[T] = rewrap(_.sortWithinPartitions(sortExprs: _*))

  @scala.annotation.varargs
  def sort(sortCol: String, sortCols: String*): TypedDataset[T] = rewrap(_.sort(sortCol, sortCols: _*))

  @scala.annotation.varargs
  def sort(sortExprs: Column*): TypedDataset[T] = rewrap(_.sort(sortExprs: _*))

  @scala.annotation.varargs
  def orderBy(sortCol: String, sortCols: String*): TypedDataset[T] = rewrap(_.orderBy(sortCol, sortCols: _*))

  @scala.annotation.varargs
  def orderBy(sortExprs: Column*): TypedDataset[T] = rewrap(_.orderBy(sortExprs: _*))

  @scala.annotation.varargs
  def hint(name: String, parameters: Any*): TypedDataset[T] = rewrap(_.hint(name, parameters: _*))

  def persist(): TypedDataset[T] = rewrap(_.persist())

  def sample(withReplacement: Boolean, fraction: Double): TypedDataset[T] = rewrap(_.sample(withReplacement, fraction))

  def sample(withReplacement: Boolean, fraction: Double, seed: Long): TypedDataset[T] = rewrap(
    _.sample(withReplacement, fraction, seed)
  )

  def select(cols: Column*): DataFrame = dataset.select(cols: _*)

  @scala.annotation.varargs
  def select(col: String, cols: String*): DataFrame = dataset.select(col, cols: _*)

  @scala.annotation.varargs
  def selectExpr(exprs: String*): DataFrame = dataset.selectExpr(exprs: _*)

  def filter(condition: TypedColumn[T, _]): TypedDataset[T] = rewrap(_.filter(condition))

  def filter(conditionExpr: String): TypedDataset[T] = rewrap(_.filter(conditionExpr))

  def schema: StructType = dataset.schema

  def printSchema(): Unit = dataset.printSchema()

  def explain(extended: Boolean): Unit = dataset.explain(extended)

  def explain(): Unit = dataset.explain()

  def dtypes: Array[(String, String)] = dataset.dtypes

  def columns: Array[String] = dataset.columns

  def isLocal: Boolean = dataset.isLocal

  def show(): Unit = dataset.show()

  def show(numRows: Int): Unit = dataset.show(numRows)

  def show(truncate: Boolean): Unit = dataset.show(truncate)

  def show(numRows: Int, truncate: Boolean): Unit = dataset.show(numRows, truncate)

  def show(numRows: Int, truncate: Int): Unit = dataset.show(numRows, truncate)

  def na: DataFrameNaFunctions = dataset.na

  def stat: DataFrameStatFunctions = dataset.stat

  def union(other: TypedDataset[T]): TypedDataset[T] = rewrap(_.union(other.dataset))

  def where(condition: TypedColumn[T, _]): TypedDataset[T] = rewrap(_.where(condition))

  def where(conditionExpr: String): TypedDataset[T] = rewrap(_.where(conditionExpr))

  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = dataset.agg(aggExpr, aggExprs: _*)

  def agg(exprs: Map[String, String]): DataFrame = dataset.agg(exprs)

  def agg(exprs: java.util.Map[String, String]): DataFrame = dataset.agg(exprs)

  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame = dataset.agg(expr, exprs: _*)

  def limit(n: Int): TypedDataset[T] = rewrap(_.limit(n))

  def intersect(other: TypedDataset[T]): TypedDataset[T] = rewrap(_.intersect(other.dataset))

  def except(other: TypedDataset[T]): TypedDataset[T] = rewrap(_.except(other.dataset))

  @scala.annotation.varargs
  def describe(cols: String*): DataFrame = dataset.describe(cols: _*)

  def head(n: Int): Array[T] = dataset.head(n)

  def head(): T = dataset.head()

  def first(): T = dataset.first()

  // Untyped variation for column expressions.
  def where(condition: Column): TypedDataset[T] = rewrap(_.where(condition))

  def filter(condition: Column): TypedDataset[T] = rewrap(_.filter(condition))

  def withColumn(colName: String, col: Column): DataFrame = dataset.withColumn(colName, col)

  def withColumnRenamed(existingName: String, newName: String): DataFrame =
    dataset.withColumnRenamed(existingName, newName)

  def drop(colName: String): DataFrame = dataset.drop(colName)

  def dropDuplicates(colNames: Seq[String]): TypedDataset[T] = rewrap(_.dropDuplicates(colNames))

  def dropDuplicates(colNames: Array[String]): TypedDataset[T] = rewrap(_.dropDuplicates(colNames))

  @scala.annotation.varargs
  def dropDuplicates(col1: String, cols: String*): TypedDataset[T] = rewrap(_.dropDuplicates(col1, cols: _*))

  def take(n: Int): Array[T] = dataset.take(n)

  def takeAsList(n: Int): java.util.List[T] = dataset.takeAsList(n)

  def collect(): Array[T] = dataset.collect()

  def collectAsList(): java.util.List[T] = dataset.collectAsList()

  def toLocalIterator: java.util.Iterator[T] = dataset.toLocalIterator()

  def repartition(numPartitions: Int): TypedDataset[T] = rewrap(_.repartition(numPartitions))

  @scala.annotation.varargs
  def repartition(numPartitions: Int, partitionExprs: Column*): TypedDataset[T] = rewrap(
    _.repartition(numPartitions, partitionExprs: _*)
  )

  @scala.annotation.varargs
  def repartition(partitionExprs: Column*): TypedDataset[T] = rewrap(_.repartition(partitionExprs: _*))

  def coalesce(numPartitions: Int): TypedDataset[T] = rewrap(_.coalesce(numPartitions))

  def distinct(): TypedDataset[T] = rewrap(_.distinct())

  def rollup(cols: Column*): RelationalGroupedDataset = dataset.rollup(cols: _*)

  def foreach(f: T => Unit): Unit = dataset.foreach(f)

  def foreach(func: ForeachFunction[T]): Unit = dataset.foreach(func)

  def foreachPartition(f: Iterator[T] => Unit): Unit = dataset.foreachPartition(f)

  def foreachPartition(func: ForeachPartitionFunction[T]): Unit = dataset.foreachPartition(func)

  def cache(): TypedDataset[T] = rewrap(_.cache())

  def persist(newLevel: StorageLevel): TypedDataset[T] = rewrap(_.persist(newLevel))

  def storageLevel: StorageLevel = dataset.storageLevel

  def unpersist(blocking: Boolean): TypedDataset[T] = rewrap(_.unpersist(blocking))

  def unpersist(): TypedDataset[T] = rewrap(_.unpersist())

  def createTempView(viewName: String): Unit = dataset.createTempView(viewName)

  def createOrReplaceTempView(viewName: String): Unit = dataset.createOrReplaceTempView(viewName)

  def createGlobalTempView(viewName: String): Unit = dataset.createGlobalTempView(viewName)

  def createOrReplaceGlobalTempView(viewName: String): Unit = dataset.createOrReplaceGlobalTempView(viewName)

  def write: DataFrameWriter[T] = dataset.write

  def writeStream: DataStreamWriter[T] = dataset.writeStream

  def toJSON: Dataset[String] = dataset.toJSON

  def inputFiles: Array[String] = dataset.inputFiles

  private def matchSchemes(schema_from: StructType, schema_to: StructType): MatchResult = {

    def findMismatch(source_schema: Array[StructField], projection_field: StructField): Option[String] =
      source_schema.collectFirst {
        case source_field if source_field.name == projection_field.name =>
          source_field
      } match {
        case Some(StructField(_, dataType, _, _)) =>
          if (dataType == projection_field.dataType) None
          else
            Some(
              s"Data types of the column '${projection_field.name}' doesn't match. Expected '$dataType', got '${projection_field.dataType}'"
            )
        case None => Some(s"Couldn't find the column '${projection_field.name}' in the source schema")
      }

    val source_schema: Array[StructField]     = schema_from.fields
    val projection_schema: Array[StructField] = schema_to.fields

    val errors: Array[String] = projection_schema.flatMap { field =>
      findMismatch(source_schema, field)
    }

    if (errors.isEmpty) MatchSuccess
    else { MatchError(errors.mkString("\n")) }
  }

  def project[U <: Product: TypeTag](implicit spark: SparkSession): Either[ProjectionException, TypedDataset[U]] =
    matchSchemes(dataset.schema, Encoders.product[U].schema) match {
      case MatchSuccess             => projectUnsafe[U].asRight
      case MatchError(errorMessage) => ProjectionException(errorMessage).asLeft
    }

  def projectUnsafe[U <: Product: TypeTag](implicit spark: SparkSession): TypedDataset[U] = {

    import spark.implicits._

    val columns = Encoders.product[U].schema.fieldNames
    dataset.select(columns.map(dataset.col): _*).as[U].typed
  }

}
