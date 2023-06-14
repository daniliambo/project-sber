package ru.sberbank.bigdata.enki.plan.expression

import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, LeafExpression, Unevaluable}
import org.apache.spark.sql.types.{ByteType, DataType}

/** Replaces [[org.apache.spark.sql.catalyst.expressions.Grouping Grouping]] */
final case class GroupingExpression(expression: Expression) extends LeafExpression with Unevaluable {
  override def nullable: Boolean  = false
  override def dataType: DataType = ByteType

  override def references: AttributeSet = expression.references

  override def sql: String = "grouping(" + expression.sql + ")"
}
