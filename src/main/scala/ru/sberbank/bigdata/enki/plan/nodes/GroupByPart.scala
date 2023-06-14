package ru.sberbank.bigdata.enki.plan.nodes

import org.apache.spark.sql.catalyst.expressions.Expression

sealed trait GroupType

object GroupType {
  final case class GroupingSets(groupByExprs: Seq[Seq[Expression]]) extends GroupType
  object GroupByType                                                extends GroupType
  object Rollup                                                     extends GroupType
  object Cube                                                       extends GroupType
}

final case class GroupByPart(groupByExpressions: Vector[Expression], groupType: GroupType)
