package ru.sberbank.bigdata.enki.plan.converter.transformers

import cats.syntax.applicative._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import ru.sberbank.bigdata.enki.plan.columns.InlineColumn
import ru.sberbank.bigdata.enki.plan.nodes.{InlineTableNode, Node, ReferenceMap}

object LocalRelationTransformer extends PlanTransformer[LocalRelation] {

  override def transform(
    plan: LocalRelation,
    children: List[Node],
    outerReferences: ReferenceMap
  ): ContextState[Node] = {
    val columns    = plan.output.map(InlineColumn(_)).toVector
    val node: Node = InlineTableNode(columns, plan.data)

    node.pure[ContextState]
  }

}
