package ru.sberbank.bigdata.enki.plan.nodes

import cats.syntax.option._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator, SortOrder}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftExistence}
import ru.sberbank.bigdata.enki.plan.columns._

/** Represents a [[org.apache.spark.sql.Dataset Dataset]] analyzed plan */
sealed abstract class Node {

  /** Output of this node */
  def columns: Vector[Column]

  def children: Vector[Node]
}

/** A node with no child */
abstract class LeafNode extends Node {
  override def children: Vector[Node] = Vector.empty
}

/** A node with exactly one child */
abstract class UnaryNode extends Node {
  def child: Node

  override def children: Vector[Node] = Vector(child)
}

/** A node with 2 children */
abstract class BinaryNode extends Node {
  def left: Node
  def right: Node

  override def children: Vector[Node] = Vector(left, right)
}

final case class SourceTableNode(tableSchema: String, tableName: String, columns: Vector[SourceColumn])
    extends LeafNode {
  def fullName: String = tableSchema + "." + tableName
}

/** Used to model ''VALUES(...)'' clause */
final case class InlineTableNode(columns: Vector[InlineColumn], rows: Seq[InternalRow]) extends LeafNode

/** Used as a source of [[SelectNode]] to model ''SELECT'' statement without ''FROM'' clause */
case object EmptyNode extends LeafNode {
  override def columns: Vector[Column] = Vector.empty
}

/** Used to assign a name to the child node */
final case class AliasedNode(alias: String, child: Node) extends UnaryNode {
  override def columns: Vector[ColumnReference] = child.columns.map(ColumnReference.fromColumn(_, alias.some))
}

trait SetOperationNode extends Node {
  def operation: String
}

/** Represents ''SELECT ... UNION (ALL) SELECT ...'' statement */
final case class UnionNode(children: Vector[SelectNode], isDistinct: Boolean = false) extends SetOperationNode {
  override def operation: String            = "UNION"
  override def columns: Vector[UnionColumn] = children.map(_.columns).transpose.map(UnionColumn(_))

  def distinct: UnionNode = copy(isDistinct = true)
}

/** Represents ''SELECT ... INTERSECT SELECT ...'' statement */
final case class IntersectNode(left: SelectNode, right: SelectNode) extends BinaryNode with SetOperationNode {
  override def operation: String = "INTERSECT"

  override def columns: Vector[UnionColumn] =
    left.columns
      .zip(right.columns)
      .map { case (leftColumn, rightColumn) => UnionColumn(leftColumn, rightColumn) }
}

/** Represents ''SELECT ... EXCEPT SELECT ...'' statement */
final case class ExceptNode(left: SelectNode, right: SelectNode) extends BinaryNode with SetOperationNode {
  override def operation: String       = "EXCEPT"
  override def columns: Vector[Column] = left.columns
}

/** Used to model joins in ''FROM'' clause */
final case class JoinNode(mainNode: AliasedNode, joins: Vector[JoinPart]) extends Node {

  override def columns: Vector[ColumnReference] =
    joins.foldLeft(mainNode.columns) { case (prevColumns, JoinPart(joinNode, joinType, _)) =>
      joinType match {
        case LeftExistence(_) => prevColumns
        case _                => prevColumns ++ joinNode.columns
      }
    }

  override def children: Vector[AliasedNode] = mainNode +: joins.map(_.node)

  def addJoin(node: AliasedNode, joinType: JoinType, condition: Option[Expression]): JoinNode = {
    val joinPart = JoinPart(node, joinType, condition)
    copy(joins = joins :+ joinPart)
  }
}

/** Used to model ''FROM'' clause with ''LATERAL VIEW'' */
final case class GeneratorNode(mainNode: Node, generators: Vector[GeneratorPart]) extends Node {

  override def columns: Vector[ColumnReference] =
    generators.foldLeft(getColumnReferences(mainNode)) {
      case (prevColumns, GeneratorPart(_, join, _, _, generatorColumns)) =>
        if (join) prevColumns ++ generatorColumns else generatorColumns
    }

  override def children: Vector[Node] = Vector(mainNode)

  def addGenerator(
    generator: Generator,
    join: Boolean,
    outer: Boolean,
    qualifier: String,
    generatorColumns: Vector[ColumnReference]
  ): GeneratorNode = {
    val generatorPart = GeneratorPart(generator, join, outer, qualifier, generatorColumns)
    copy(generators = generators :+ generatorPart)
  }
}

/** Represents ''SELECT'' statement */
final case class SelectNode(
  columns: Vector[Column],
  source: Node,
  columnsFromSource: Boolean               = true,
  whereClause: Option[Expression]          = None,
  groupByClause: Option[GroupByPart]       = None,
  havingClause: Option[Expression]         = None,
  orderByClause: Option[Vector[SortOrder]] = None,
  limit: Option[Expression]                = None,
  isDistinct: Boolean                      = false
) extends UnaryNode {

  override def child: Node = source

  def withColumns(newColumns: Vector[Column]): SelectNode =
    copy(columns = newColumns, columnsFromSource = false)

  def withWhereClause(expression: Expression): SelectNode =
    copy(whereClause = expression.some)

  def withGroupByClause(expressions: Vector[Expression], groupType: GroupType): SelectNode = {
    val groupByPart = GroupByPart(expressions, groupType)
    copy(groupByClause = groupByPart.some)
  }

  def withHavingClause(expression: Expression): SelectNode =
    copy(havingClause = expression.some)

  def withOrderByClause(expressions: Vector[SortOrder]): SelectNode =
    copy(orderByClause = expressions.some)

  def withLimit(expression: Expression): SelectNode =
    copy(limit = expression.some)

  def distinct: SelectNode = copy(isDistinct = true)

  def addColumns(columnsToAdd: Vector[Column]): SelectNode =
    copy(columns = columns ++ columnsToAdd, columnsFromSource = false)
}

object SelectNode {

  def apply(source: Node): SelectNode =
    source match {
      case selectNode: SelectNode =>
        selectNode
      case otherNode =>
        SelectNode(getColumnReferences(otherNode), otherNode)
    }
}
