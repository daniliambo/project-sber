package ru.sberbank.bigdata.enki.plan.converter.transformers

import cats.data.State
import org.apache.spark.sql.catalyst.plans.logical.Distinct
import ru.sberbank.bigdata.enki.plan.nodes.{Node, ReferenceMap, SelectNode, UnionNode}

private[converter] object DistinctTransformer extends PlanTransformer[Distinct] {

  override def transform(plan: Distinct, children: List[Node], outerReferences: ReferenceMap): ContextState[Node] =
    children.head match {
      case unionNode: UnionNode =>
        State.pure(
          unionNode.distinct
        )

      case selectNode: SelectNode if selectNode.orderByClause.isEmpty && selectNode.limit.isEmpty =>
        State.pure(
          selectNode.distinct
        )

      case otherNode => wrapInSelect(otherNode).map(_.distinct)
    }
}
