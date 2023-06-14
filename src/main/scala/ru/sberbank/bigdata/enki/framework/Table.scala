package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Encoder, Row, SparkSession}
import ru.sberbank.bigdata.enki.schema.withMetadataFromAnnotations

import scala.collection.breakOut
import scala.reflect.runtime.universe._

// TODO - fix inconsistent behavior when T is not Row and structMap is defined
abstract class Table[T: TypeTag] extends TableStore[T] with TableName {

  val structMap: Option[Map[String, String]] = None

  /** StructType of [[org.apache.spark.sql.Dataset]], representing structure of [[T]]
    * Expected to be provided via [[structMap]] when [[T]] equals [[org.apache.spark.sql.Row]]
    */
  final lazy val structType: Option[StructType] = structMap.map(structFromMap)

  def exists(implicit spark: SparkSession): Boolean =
    exists(TableLocation(qualifiedName, path))

  def hasData(implicit spark: SparkSession): Boolean =
    hasData(TableLocation(qualifiedName, path))

  private def getEmptyFromClassLoader(classLoader: Option[ClassLoader])(implicit spark: SparkSession): Dataset[T] =
    classLoader match {
      case Some(loader) =>
        val defaultLoader = Thread.currentThread().getContextClassLoader
        Thread.currentThread().setContextClassLoader(loader)
        val ds = withMetadataFromAnnotations(spark.emptyDataset[T](ExpressionEncoder()))
        Thread.currentThread().setContextClassLoader(defaultLoader)
        ds
      case None => withMetadataFromAnnotations(spark.emptyDataset[T](ExpressionEncoder()))
    }

  /** Return empty dataset of given structure.
    * TypeTag of Dataset has the first priority,
    * then goes [[org.apache.spark.sql.types.StructType]] derived from [[structMap]]
    * If TypeTag is Row and no schema provided,
    * then default spark behaviour for dataset with no structure is called
    *
    * @param schemaOpt StructType provided by user via [[structMap]]
    * @param classLoader Optional classloader, for calls from sbt plugin
    * @param spark SparkSession
    * @return Empty Dataset with type T
    */
  def getEmpty(schemaOpt: Option[StructType] = structType, classLoader: Option[ClassLoader] = None)(
    implicit spark: SparkSession
  ): Dataset[T] = schemaOpt match {
    case None if typeOf[T] != typeOf[Row] => getEmptyFromClassLoader(classLoader)

    case Some(s) => // hack until CaseClassGenerator is fixed or structType behavior is changed
      spark.emptyDataset[T](RowEncoder(s).asInstanceOf[Encoder[T]])

    case _ => spark.emptyDataset[T](encoder(new StructType())) // same behaviour as spark.emptyDataFrame
  }

  def getTypeParameter: Type = typeOf[T]

  def write(mode: WriteMode, data: Dataset[T])(implicit spark: SparkSession): Unit =
    write(TableLocation(qualifiedName, path), mode, data)

  private[framework] def structFromMap(map: Map[String, String]): StructType =
    StructType(map.map { case (name, dataType) =>
      StructField(name, CatalystSqlParser.parseDataType(dataType))
    }(breakOut))
}
