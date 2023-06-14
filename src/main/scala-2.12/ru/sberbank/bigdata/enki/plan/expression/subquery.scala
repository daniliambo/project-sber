package ru.sberbank.bigdata.enki.plan.expression

import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression, Predicate, Unevaluable}
import org.apache.spark.sql.types.DataType
import ru.sberbank.bigdata.enki.plan.nodes.Node
import ru.sberbank.bigdata.enki.plan.printer.Printer

/** A base trait for expressions that replace
  * [[org.apache.spark.sql.catalyst.expressions.SubqueryExpression SubqueryExpression]]'s successors
  */
sealed trait Subquery extends LeafExpression with Unevaluable {
  def node: Node

  // Temporary solution until printExpression in Printer changes
  override def sql: String = {
    val firstLine = this match {
      case _: ScalarSubquery => "(\n"

      case inSubquery: InSubquery => s"${inSubquery.value.sql} IN (\n"

      case _: ExistsSubquery => "EXISTS (\n"
    }

    val nodeSql = Printer.default.print(node)
    val rows    = nodeSql.split('\n')

    val indent = " " * 4

    val builder = StringBuilder.newBuilder

    builder.append(firstLine)
    rows.foreach(row => builder.append(indent + row + "\n"))
    builder.append(")")

    builder.toString()
  }
}

/** Replacement for [[org.apache.spark.sql.catalyst.expressions.ScalarSubquery ScalarSubquery]] expression */
final case class ScalarSubquery(node: Node, dataType: DataType) extends Subquery {
  override def nullable: Boolean = true
}

/** Replacement for [[org.apache.spark.sql.catalyst.expressions.InSubquery InSubquery]]`(..., `[[org.apache.spark.sql.catalyst.expressions.ListQuery ListQuery]]`(...)))`
  * expression
  */
final case class InSubquery(value: Expression, node: Node) extends Subquery with Predicate {
  override def nullable: Boolean = false
}

/** Replacement for [[org.apache.spark.sql.catalyst.expressions.Exists Exists]] expression */
final case class ExistsSubquery(node: Node) extends Subquery with Predicate {
  override def nullable: Boolean = false
}
