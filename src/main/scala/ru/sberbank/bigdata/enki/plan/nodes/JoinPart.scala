package ru.sberbank.bigdata.enki.plan.nodes

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType

final case class JoinPart(node: AliasedNode, joinType: JoinType, condition: Option[Expression])
