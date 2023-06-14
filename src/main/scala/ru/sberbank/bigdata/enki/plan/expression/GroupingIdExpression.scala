package ru.sberbank.bigdata.enki.plan.expression

import org.apache.spark.sql.catalyst.expressions.{LeafExpression, Unevaluable}
import org.apache.spark.sql.types.{DataType, IntegerType}

/** Replaces [[org.apache.spark.sql.catalyst.expressions.GroupingID GroupingID]] */
case object GroupingIdExpression extends LeafExpression with Unevaluable {
  override def nullable: Boolean  = false
  override def dataType: DataType = IntegerType

  override def sql: String = "grouping_id()"
}
