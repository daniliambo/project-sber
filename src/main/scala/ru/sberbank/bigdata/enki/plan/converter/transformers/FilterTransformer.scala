package ru.sberbank.bigdata.enki.plan.converter.transformers

import cats.syntax.applicative._
import org.apache.spark.sql.catalyst.plans.logical.Filter
import ru.sberbank.bigdata.enki.plan.nodes.{getColumnReferencesMap, Node, ReferenceMap, SelectNode}

private[converter] object FilterTransformer extends PlanTransformer[Filter] {

  override def transform(plan: Filter, children: List[Node], outerReferences: ReferenceMap): ContextState[Node] =
    for {
      nodeWithInfo <- children.head match {
        case selectNode @ SelectNode(_, _, _, _, Some(_), None, None, None, false) => // Having clause
          (selectNode, true).pure[ContextState]

        case selectNode @ SelectNode(_, _, true, None, None, None, None, None, false) =>
          (selectNode, false).pure[ContextState]

        case otherNode => wrapInSelect(otherNode).map(_ -> false)
      }
      aliasContext <- getContext
    } yield {
      val (selectNode, isHavingClause) = nodeWithInfo

      val references       = getColumnReferencesMap(selectNode) ++ outerReferences
      val updatedCondition = updateExpression(plan.condition, references, aliasContext)

      if (isHavingClause)
        selectNode.withHavingClause(updatedCondition)
      else
        selectNode.withWhereClause(updatedCondition)
    }

}
