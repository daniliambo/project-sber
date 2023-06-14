package ru.sberbank.bigdata.enki.plan.columns

import cats.syntax.option._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.types.DataType

/** A column of a [[ru.sberbank.bigdata.enki.plan.nodes.Node Node]] */
sealed abstract class Column {

  /** A unique identifier assigned to this column */
  def id: Long

  def name: String

  def dataType: DataType

  def nullable: Boolean

  /** All [[SourceColumn]]s referenced in this column */
  def sourceColumns: Vector[SourceColumn]
}

final case class SourceColumn(id: Long, name: String, dataType: DataType, nullable: Boolean) extends Column {
  override def sourceColumns: Vector[SourceColumn] = Vector(this)
}

object SourceColumn {

  def apply(attribute: Attribute): SourceColumn =
    SourceColumn(attribute.exprId.id, attribute.name, attribute.dataType, attribute.nullable)
}

final case class InlineColumn(id: Long, name: String, dataType: DataType, nullable: Boolean) extends Column {
  override def sourceColumns: Vector[SourceColumn] = Vector.empty
}

object InlineColumn {

  def apply(attribute: Attribute): InlineColumn =
    InlineColumn(attribute.exprId.id, attribute.name, attribute.dataType, attribute.nullable)
}

/** A wrapped [[org.apache.spark.sql.catalyst.expressions.Expression Expression]] with a name */
final case class ExpressionColumn(id: Long, name: String, expression: Expression, sourceColumns: Vector[SourceColumn])
    extends Column {

  override def dataType: DataType = expression.dataType
  override def nullable: Boolean  = expression.nullable
}

/** A reference to the other column
  * @param column    referenced column
  * @param qualifier optional qualifier (i.e. table name)
  * @param replaceId in some cases reference id may differ from the original column one
  */
final case class ColumnReference(column: Column, qualifier: Option[String], replaceId: Option[Long]) extends Column {

  override def id: Long                            = replaceId.getOrElse(column.id)
  override def name: String                        = column.name
  override def dataType: DataType                  = column.dataType
  override def nullable: Boolean                   = column.nullable
  override def sourceColumns: Vector[SourceColumn] = column.sourceColumns

  def withId(newId: Long): ColumnReference =
    if (newId == id) {
      this
    } else {
      copy(replaceId = newId.some)
    }

  def withQualifier(newQualifier: Option[String]): ColumnReference =
    if (newQualifier == qualifier) {
      this
    } else {
      copy(qualifier = newQualifier)
    }
}

object ColumnReference {

  def fromColumn(column: Column, qualifier: Option[String] = None): ColumnReference =
    column match {
      case ref: ColumnReference =>
        val newQualifier = qualifier.orElse(ref.qualifier)
        ref.withQualifier(newQualifier)

      case other => ColumnReference(other, qualifier, None)
    }

}

/** Combines multiple columns into one */
final case class UnionColumn(columns: Vector[Column]) extends Column {
  private def head: Column = columns.head

  override def id: Long                            = head.id
  override def name: String                        = head.name
  override def dataType: DataType                  = head.dataType
  override def nullable: Boolean                   = columns.exists(_.nullable)
  override def sourceColumns: Vector[SourceColumn] = columns.flatMap(_.sourceColumns).distinct
}

object UnionColumn {
  def apply(column: Column, columns: Column*): UnionColumn = UnionColumn((column +: columns).toVector)
}
