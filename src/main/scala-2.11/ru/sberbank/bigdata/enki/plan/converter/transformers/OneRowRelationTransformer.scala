package ru.sberbank.bigdata.enki.plan.converter.transformers

import cats.syntax.applicative._
import org.apache.spark.sql.catalyst.plans.logical.OneRowRelation
import ru.sberbank.bigdata.enki.plan.nodes.{EmptyNode, Node, ReferenceMap}

object OneRowRelationTransformer extends PlanTransformer[OneRowRelation.type] {

  override def transform(
    plan: OneRowRelation.type,
    children: List[Node],
    outerReferences: ReferenceMap
  ): ContextState[Node] = (EmptyNode: Node).pure[ContextState]

}
