package ru.sberbank.bigdata.enki.plan.printer

import org.apache.spark.sql.catalyst.expressions.{And, Expression, Not, Or}
import ru.sberbank.bigdata.enki.plan.columns._
import ru.sberbank.bigdata.enki.plan.nodes._
import ru.sberbank.bigdata.enki.plan.printer.expressions._
import ru.sberbank.bigdata.enki.plan.printer.util.appendToBuilder

import scala.annotation.tailrec
import scala.collection.immutable.Queue

final case class Printer(useCTE: Boolean, indent: Int) {
  /* Sql keywords */
  private val valuesText       = "VALUES"
  private val withText         = "WITH"
  private val selectText       = "SELECT"
  private val distinctText     = "DISTINCT"
  private val fromText         = "FROM"
  private val whereText        = "WHERE"
  private val groupByText      = "GROUP BY"
  private val groupingSetsText = "GROUPING SETS"
  private val rollupText       = "WITH ROLLUP"
  private val cubeText         = "WITH CUBE"
  private val havingClause     = "HAVING"
  private val orderByText      = "ORDER BY"
  private val limitText        = "LIMIT"
  private val allText          = "ALL"
  private val joinText         = "JOIN"
  private val onText           = "ON"
  private val lateralViewText  = "LATERAL VIEW"
  private val outerText        = "OUTER"

  def print(node: Node): String = {
    val builder = new StringBuilder

    if (useCTE) {
      val foundAliases = extractAliasedSubqueries(node)
      if (foundAliases.nonEmpty) addCTE(foundAliases, builder)
    }

    addNode(node, builder, depth = 0)

    builder.toString()
  }

  def printExpression(expression: Expression): String = {
    val builder = new StringBuilder

    addExpression(expression, builder, depth = 0, newline = false)

    builder.toString()
  }

  /* Methods to extract and add CTE to the builder */
  private def extractAliasedSubqueries(node: Node): List[AliasedNode] = {
    @tailrec
    def go(nodes: Queue[Node], acc: List[AliasedNode]): List[AliasedNode] =
      if (nodes.nonEmpty) {
        val (head, rest) = nodes.dequeue
        head match {
          case aliased: AliasedNode =>
            aliased.child match {
              case _: SelectNode | _: UnionNode | _: IntersectNode | _: ExceptNode =>
                go(rest.enqueue(aliased.child), aliased :: acc)
              case _ =>
                go(rest, acc)
            }
          case other => go(rest.enqueue(other.children), acc)
        }
      } else {
        acc
      }

    go(Queue(node), Nil)
  }

  private def addCTE(nodes: List[AliasedNode], builder: StringBuilder): Unit = {
    @tailrec
    def go(nodes: List[AliasedNode]): Unit =
      nodes match {
        case aliased :: rest =>
          val startLine = aliased.alias + " AS "
          appendToBuilder(startLine, builder, depth = 0, newline = false)

          addWrappedInBraces(aliased.child, builder, depth = 0, newline = false)

          rest match {
            case _ :: _ => appendToBuilder(",", builder, depth = 0); go(rest)
            case _      => appendToBuilder("", builder, depth = 0)
          }

        case Nil => ()
      }

    appendToBuilder(withText + " ", builder, depth = 0, newline = false)
    go(nodes)
  }

  /* Methods to add nodes */
  private def addNode(node: Node, builder: StringBuilder, depth: Int): Unit =
    node match {
      case sourceTableNode: SourceTableNode   => addSourceTableNode(sourceTableNode, builder)
      case inlineTableNode: InlineTableNode   => addInlineTableNode(inlineTableNode, builder)
      case aliasedNode: AliasedNode           => addAliasedNode(aliasedNode, builder, depth)
      case setOperationNode: SetOperationNode => addSetOperationNode(setOperationNode, builder, depth)
      case joinNode: JoinNode                 => addJoinNode(joinNode, builder, depth)
      case generatorNode: GeneratorNode       => addGeneratorNode(generatorNode, builder, depth)
      case selectNode: SelectNode             => addProjectNode(selectNode, builder, depth)
    }

  private def addWrappedInBraces(node: Node, builder: StringBuilder, depth: Int, newline: Boolean = true): Unit = {
    appendToBuilder("(", builder, 0)
    addNode(node, builder, depth + indent)
    appendToBuilder(")", builder, depth, newline)
  }

  private def addSourceTableNode(node: SourceTableNode, builder: StringBuilder): Unit = {
    val sql = node.fullName

    appendToBuilder(sql, builder, depth = 0, newline = false)
  }

  private def addInlineTableNode(node: InlineTableNode, builder: StringBuilder): Unit = {
    val dataTypes = node.columns.map(_.dataType)
    val sql = node.rows
      .map(_.toSeq(dataTypes).mkString("(", ", ", ")"))
      .mkString(valuesText + " ", ", ", "")

    appendToBuilder(sql, builder, depth = 0, newline = false)
  }

  private def addAliasedNode(node: AliasedNode, builder: StringBuilder, depth: Int): Unit = {
    val aliasSql = node.alias

    node.child match {
      case source: SourceTableNode =>
        addSourceTableNode(source, builder)
        appendToBuilder(" " + aliasSql, builder, depth = 0)

      case inline: InlineTableNode =>
        addInlineTableNode(inline, builder)
        val columnNames = inline.columns.map(_.name).mkString(", ")
        val sql         = s" AS $aliasSql($columnNames)"
        appendToBuilder(sql, builder, depth = 0)

      // In that case node was already added as a common table expression
      case _ if useCTE => appendToBuilder(aliasSql, builder, depth = 0)

      case other =>
        addWrappedInBraces(other, builder, depth, newline = false)
        appendToBuilder(" " + aliasSql, builder, depth    = 0)
    }
  }

  private def addSetOperationNode(node: SetOperationNode, builder: StringBuilder, depth: Int): Unit = {
    val sql = node match {
      case union: UnionNode if !union.isDistinct => union.operation + " " + allText
      case other                                 => other.operation
    }
    val children       = node.children
    val childrenLength = children.length

    @tailrec
    def go(idx: Int): Unit = {
      if (idx > 0) appendToBuilder(sql, builder, depth)

      addNode(children(idx), builder, depth)

      if (idx < childrenLength - 1) go(idx + 1)
    }

    go(0)
  }

  private def addJoinNode(node: JoinNode, builder: StringBuilder, depth: Int): Unit = {
    addNode(node.mainNode, builder, depth)

    node.joins.foreach { case JoinPart(joinNode, joinType, maybeExpression) =>
      val joinSql = joinType.sql + " " + joinText + " "
      appendToBuilder(joinSql, builder, depth, newline = false)

      addAliasedNode(joinNode, builder, depth)

      maybeExpression.foreach { expr =>
        val onSql = onText + " "
        appendToBuilder(onSql, builder, depth, newline = false)

        addExpression(expr, builder, depth = 0)
      }
    }
  }

  private def addGeneratorNode(node: GeneratorNode, builder: StringBuilder, depth: Int): Unit = {
    addNode(node.mainNode, builder, depth)

    node.generators.foreach { case GeneratorPart(generator, _, outer, alias, columns) =>
      val start = if (outer) lateralViewText + " " + outerText else lateralViewText
      appendToBuilder(start + " ", builder, depth, newline = false)

      addExpression(generator, builder, depth = 0, newline = false)

      val aliasSql = " " + alias + " AS "
      val end      = columns.map(_.name).mkString(aliasSql, ", ", "")
      appendToBuilder(end, builder, depth = 0)
    }
  }

  private def addProjectNode(node: SelectNode, builder: StringBuilder, depth: Int): Unit = {
    val selectSql = if (node.isDistinct) selectText + " " + distinctText else selectText
    appendToBuilder(selectSql + " ", builder, depth, newline = false)
    addColumns(node.columns, builder, depth + selectSql.length + 1)

    node.source match {
      case EmptyNode => ()

      case _: AliasedNode | _: JoinNode | _: GeneratorNode =>
        appendToBuilder(fromText + " ", builder, depth, newline = false)
        addNode(node.source, builder, depth)

      case otherNode =>
        appendToBuilder(fromText + " (", builder, depth)
        addNode(otherNode, builder, depth + indent)
        appendToBuilder(")", builder, depth)
    }

    def addClauseWithExpressions(clause: String, exprs: Vector[Expression]): Unit = {
      appendToBuilder(clause + " ", builder, depth, newline = false)
      addExpressionsSeq(exprs, builder, depth + clause.length + 1)
    }

    node.whereClause.foreach(expr => addClauseWithExpressions(whereText, Vector(expr)))

    node.groupByClause.foreach { case GroupByPart(exprs, groupType) =>
      addClauseWithExpressions(groupByText, exprs)
      groupType match {
        case GroupType.GroupByType =>
          ()

        case GroupType.Cube =>
          appendToBuilder(cubeText, builder, depth)

        case GroupType.Rollup =>
          appendToBuilder(rollupText, builder, depth)

        case GroupType.GroupingSets(groupByExprs) =>
          val sql = groupByExprs
            .map(_.map(_.sql).mkString("(", ", ", ")"))
            .mkString(groupingSetsText + " (", ", ", ")")
          appendToBuilder(sql, builder, depth)
      }
    }

    node.havingClause.foreach(expr => addClauseWithExpressions(havingClause, Vector(expr)))

    node.orderByClause.foreach(exprs => addClauseWithExpressions(orderByText, exprs))

    node.limit.foreach(expr => addClauseWithExpressions(limitText, Vector(expr)))
  }

  /* Columns */
  private def addColumns(columns: Vector[Column], builder: StringBuilder, depth: Int): Unit =
    columns.zipWithIndex.foreach { case (column, idx) =>
      if (idx == 0) {
        addColumn(column, builder, depth = 0, newline = false)
      } else {
        addColumn(column, builder, depth, newline = false)
      }

      if (idx < columns.length - 1) {
        appendToBuilder(",", builder, depth = 0)
      } else {
        appendToBuilder("", builder, depth = 0)
      }
    }

  @tailrec
  private def addColumn(column: Column, builder: StringBuilder, depth: Int, newline: Boolean = true): Unit =
    column match {
      case SourceColumn(_, name, _, _) =>
        appendToBuilder(name, builder, depth, newline)

      case InlineColumn(_, name, _, _) =>
        appendToBuilder(name, builder, depth, newline)

      case ExpressionColumn(_, name, expression, _) =>
        val asSql = " AS " + name
        addExpression(expression, builder, depth, newline = false)
        appendToBuilder(asSql, builder, depth             = 0, newline)

      case ColumnReference(column, qualifier, _) =>
        val sql = qualifier
          .map(q => q + "." + column.name)
          .getOrElse(column.name)
        appendToBuilder(sql, builder, depth, newline)

      case UnionColumn(columns) =>
        addColumn(ColumnReference.fromColumn(columns.head), builder, depth, newline)
    }

  /* Spark Expressions */
  private def addExpressionsSeq(exprs: Vector[Expression],
                                builder: StringBuilder,
                                depth: Int,
                                newline: Boolean = true
  ): Unit =
    exprs.iterator.zipWithIndex.foreach { case (expr, idx) =>
      if (idx == 0) {
        addExpression(expr, builder, depth = 0, newline = false)
      } else {
        addExpression(expr, builder, depth, newline = false)
      }

      if (idx < exprs.length - 1) {
        appendToBuilder(",", builder, depth = 0)
      } else {
        appendToBuilder("", builder, depth = 0, newline)
      }
    }

  private def addExpression(expression: Expression,
                            builder: StringBuilder,
                            depth: Int,
                            newline: Boolean = true
  ): Unit = {
    val transformed = expression.transformDown {
      case and: And =>
        val children = flattenBinaryOperator[And](and)
        AndWrapper(children)

      case or: Or =>
        val children = flattenBinaryOperator[Or](or)
        OrWrapper(children)

      case not: Not =>
        NotWrapper(not.child)
    }

    val rows = transformed.sql.split("\n")

    rows.iterator.zipWithIndex.foreach { case (row, idx) =>
      if (idx < rows.length - 1) appendToBuilder(row, builder, depth)
      else appendToBuilder(row, builder, depth, newline)
    }
  }

}

object Printer {
  def default: Printer    = Printer(useCTE = false, indent = 4)
  def defaultCte: Printer = Printer(useCTE = true, indent = 4)
}
