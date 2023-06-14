package ru.sberbank.bigdata.enki.difftool

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, StructField}

protected trait DiffTool {

  private val statusColName = "status"
  private val addedStatus   = "added"
  private val removedStatus = "removed"
  private val updatedStatus = "updated"
  private val oldValue      = "old"
  private val newValue      = "new"

  private val addedStatusFormat   = "+++"
  private val removedStatusFormat = "---"
  private val updatedStatusFormat = "->"
  private val diffLiteralFormat   = "|->|"

  def diff(left: Dataset[_], right: Dataset[_], ignoreDecimalPrecision: Boolean, keyColumns: Seq[String]): Dataset[_] =
    formatDiff(dfDiff(left, right, ignoreDecimalPrecision, keyColumns))

  private def dfDiff(
    left: Dataset[_],
    right: Dataset[_],
    ignoreDecimalPrecision: Boolean,
    keyColumns: Seq[String]
  ): Dataset[_] = {

    val fields = (f: StructField) =>
      (f.name,
       f.dataType match {
         case _: DecimalType if ignoreDecimalPrecision => "DecimalType"
         case s                                        => s.toString
       }
      )

    val leftSchema  = left.schema.map(fields)
    val rightSchema = right.schema.map(fields)

    if (leftSchema != rightSchema) {
      throw new IllegalArgumentException(s"""
                                            |Datasets have different columns!
                                            |left:  ${leftSchema.diff(rightSchema).mkString(", ")}
                                            |right: ${rightSchema.diff(leftSchema).mkString(", ")}
         """.stripMargin)
    }

    val columns = left.schema.map(_.name.toLowerCase)

    val added   = keyColumns.map(left(_).isNull).reduce(_.and(_))
    val removed = keyColumns.map(right(_).isNull).reduce(_.and(_))

    def different(name: String): Column = not(left(name) <=> right(name))

    def colDiff(name: String): Column = struct(
      left(name).as(oldValue),
      right(name).as(newValue),
      when(added, addedStatus)
        .when(removed, removedStatus)
        .when(different(name), updatedStatus)
        .as(statusColName)
    )

    left
      .join(right, keyColumns.map(c => left(c) === right(c)).reduce(_.and(_)), "outer")
      .select(columns.map(c => colDiff(c).as(c)): _*)
      .withColumn(statusColName, coalesce(columns.map(c => col(s"$c.$statusColName")): _*))
  }

  private def formatDiff(diff: Dataset[_]): Dataset[_] = {
    def formatActionColumn: Column =
      when(diff(statusColName) === addedStatus, addedStatusFormat)
        .when(diff(statusColName) === removedStatus, removedStatusFormat)
        .when(diff(statusColName) === updatedStatus, updatedStatusFormat)

    def formatDiffColumn(colName: String): Column =
      when(diff(s"$colName.$statusColName") === addedStatus, diff(s"$colName.$newValue"))
        .when(diff(s"$colName.$statusColName") === removedStatus, diff(s"$colName.$oldValue"))
        .when(
          diff(s"$colName.$statusColName") === updatedStatus,
          concat(diff(s"$colName.$oldValue"), lit(diffLiteralFormat), diff(s"$colName.$newValue"))
        )
        .otherwise(diff(s"$colName.$oldValue"))

    diff
      .select(
        formatActionColumn.as(statusColName) +: diff.schema
          .filter(_.name != statusColName)
          .map(c => formatDiffColumn(c.name).as(c.name)): _*
      )
      .where(col(statusColName).isNotNull)
  }

}

object DiffTool extends DiffTool {

  implicit class DataFrameExtensions(dataFrame: Dataset[_]) {

    def diff(other: Dataset[_], keyColumn: String, keyColumns: String*): Dataset[_] =
      DiffTool.diff(dataFrame, other, ignoreDecimalPrecision = false, keyColumn +: keyColumns)

    def diff(other: Dataset[_], ignoreDecimalPrecision: Boolean, keyColumn: String, keyColumns: String*): Dataset[_] =
      DiffTool.diff(dataFrame, other, ignoreDecimalPrecision, keyColumn +: keyColumns)
  }

}
