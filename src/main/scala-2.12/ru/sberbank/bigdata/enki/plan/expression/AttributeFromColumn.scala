package ru.sberbank.bigdata.enki.plan.expression

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType
import ru.sberbank.bigdata.enki.plan.columns.ColumnReference

/** Replaces [[org.apache.spark.sql.catalyst.expressions.Attribute Attribute]] */
final case class AttributeFromColumn(column: ColumnReference)
    extends LeafExpression
    with NamedExpression
    with Unevaluable {

  override def name: String           = column.name
  override def qualifier: Seq[String] = column.qualifier.toList
  override def dataType: DataType     = column.dataType
  override def nullable: Boolean      = column.nullable
  override def exprId: ExprId         = ExprId(column.id)

  override def sql: String = column.qualifier.fold(name)(_ + "." + name)

  override def newInstance(): NamedExpression = this

  override def toAttribute: Attribute = AttributeReference(name, dataType, nullable)(exprId, qualifier)

  override def references: AttributeSet = AttributeSet(toAttribute)
}
