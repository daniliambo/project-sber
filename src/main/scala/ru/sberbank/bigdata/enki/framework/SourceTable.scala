package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, Row, SparkSession}
import org.apache.spark.sql.functions._
import ru.sberbank.bigdata.enki.sql.Functions._

import scala.reflect.runtime.universe._

abstract class SourceTable[T: TypeTag] extends Table[T] with SourceView[T] {
  override def get(implicit spark: SparkSession): Dataset[T] = read(TableLocation(qualifiedName, None))

  override def get(replaceMissingColsWithNull: Boolean = false)(implicit spark: SparkSession): Dataset[T] =
    if (replaceMissingColsWithNull) {
      readSafe(TableLocation(qualifiedName, path))
    } else {
      get
    }

  private def matchSchemes(sourceScheme: StructType, requiredScheme: StructType): MatchResult = {

    def findMismatch(sourceCols: Array[StructField], requiredField: StructField): Option[StructField] =
      sourceCols.collectFirst {
        case field if field.name.toLowerCase == requiredField.name.toLowerCase => field
      } match {
        case Some(_) => None
        case None    => Some(requiredField)
      }

    val sourceCols: Array[StructField]   = sourceScheme.fields
    val requiredCols: Array[StructField] = requiredScheme.fields

    val fields: Array[StructField] = requiredCols.flatMap { field =>
      findMismatch(sourceCols, field)
    }
    if (fields.isEmpty) MatchSuccess
    else { UnmatchedFields(fields) }
  }

  private def addMissingColumns(sourceDF: DataFrame, fields: Array[StructField])(
    implicit spark: SparkSession
  ): Dataset[T] = {
    val missingCols: Array[Column] = fields.map(field => lit(null).cast(field.dataType).as(field.name))

    val correctDF = sourceDF
      .select(sourceDF.columns.map(col) ++ missingCols: _*)
      .select(classAccessors[T].reverse.map(col): _*)

    fields.foreach { field =>
      warn(
        s"The column '${field.name}' not found in the scheme of the source table '$qualifiedName'. Replacing with NULL value."
      )
    }
    decode(correctDF)
  }

  private def readSafe(location: Location)(implicit session: SparkSession): Dataset[T] = {
    val dataFrame: DataFrame = readDf(location)

    implicit def encoder: Encoder[T] = EncoderFromTag[T](dataFrame.schema)

    matchSchemes(dataFrame.schema, session.emptyDataset[T].schema) match {
      case MatchSuccess            => decode(dataFrame)
      case UnmatchedFields(fields) => addMissingColumns(dataFrame, fields)
    }
  }
}
