package ru.sberbank.bigdata.enki.plan.converter.transformers

import cats.syntax.applicative._
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.Sort
import ru.sberbank.bigdata.enki.plan.nodes.{getColumnReferencesMap, Node, ReferenceMap, SelectNode}

private[converter] object SortTransformer extends PlanTransformer[Sort] {

  override def transform(plan: Sort, children: List[Node], outerReferences: ReferenceMap): ContextState[Node] =
    for {
      selectNode <- children.head match {
        case selectNode: SelectNode if selectNode.orderByClause.isEmpty && selectNode.limit.isEmpty =>
          selectNode.pure[ContextState]

        case otherNode => wrapInSelect(otherNode)
      }
      aliasContext <- getContext
    } yield {
      val references = getColumnReferencesMap(selectNode)

      val updatedOrders = plan.order
        .map(updateExpression(_, references, aliasContext).asInstanceOf[SortOrder] /* Safe */ )
        .toVector

      selectNode.withOrderByClause(updatedOrders)
    }

}
