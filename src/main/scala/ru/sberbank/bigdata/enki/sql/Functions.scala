package ru.sberbank.bigdata.enki.sql

import java.sql.Timestamp
import org.apache.spark.sql.{Column, Encoder}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions._
import ru.sberbank.bigdata.enki.sql.TypedDatasetSyntax._
import ru.sberbank.bigdata.enki.sql.Types.TypeMapping
import ru.sberbank.bigdata.enki.syntax.timestamp._
import scala.reflect.runtime.universe._

/** Additional functions for DataFrame operations.
  */
object Functions {

  /** Replace null value with zero.
    */
  def null_to_zero(col: Column): Column =
    map_column(col, coalesce(_, lit(0)))

  def null_to_false(col: Column): Column =
    map_column(col, coalesce(_, lit(false)))

  /** Apply expression to column preserving column's name if any.
    */
  def map_column(col: Column, f: Column => Column): Column =
    col.expr match {
      case named: NamedExpression => f(col).as(named.name)
      case _                      => f(col)
    }

  /** Convert decimal type to plain string stripping trailing zeros.
    */
  def to_plain_string_strip_trailing_zeros(col: Column): Column = {
    val f = udf { x: java.math.BigDecimal =>
      Option(x).map(_.stripTrailingZeros.toPlainString)
    }
    map_column(col, f(_))
  }

  /** Null column with type information.
    */
  def typed_null[T: TypeMapping]: Column =
    lit(null).cast(implicitly[TypeMapping[T]].dataType)

  /** Trim column then replace whitespace with null.
    */
  def trim_to_null(col: Column): Column =
    map_column(col,
               { c =>
                 val trimmed = trim(col)
                 when(trimmed === lit(""), lit(null)).otherwise(trimmed)
               }
    )

  /** Add number of years.
    */
  def add_years(col: Column, numYears: Int): Column = {
    val addYearsUdf = udf { dateOrNull: Timestamp =>
      Option(dateOrNull).map(_.plusYears(numYears))
    }
    map_column(col, addYearsUdf(_))
  }

  implicit class ColumnExtensions(col: Column) {

    def <=!=>(other: Any): Column =
      not(col <=> other)
  }

  def first_day(col: Column): Column = {
    val firstDayUdf = udf { dateOrNull: Timestamp =>
      Option(dateOrNull).map(_.atStartOfMonth)
    }
    map_column(col, firstDayUdf(_))
  }

  def broadcast_typed[T: Encoder](df: TypedDataset[T]): TypedDataset[T] =
    broadcast(df.dataset).typed

  def classAccessors[T: TypeTag]: List[String] = typeOf[T].members.collect {
    case m: MethodSymbol if m.isCaseAccessor => m.name.toString
  }.toList
}
