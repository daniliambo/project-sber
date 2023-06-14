package ru.sberbank.bigdata.enki.plan.converter.transformers

import cats.syntax.applicative._
import org.apache.spark.sql.catalyst.plans.logical.Join
import ru.sberbank.bigdata.enki.plan.nodes.{getColumnReferencesMap, JoinNode, Node, ReferenceMap}

private[converter] object JoinTransformer extends PlanTransformer[Join] {

  override def transform(plan: Join, children: List[Node], outerReferences: ReferenceMap): ContextState[Node] = {
    val List(leftNode, rightNode) = children

    for {
      mainNode <- leftNode match {
        case joinNode: JoinNode => joinNode.pure[ContextState]

        case otherNode => wrapInAliased(otherNode).map(JoinNode(_, Vector.empty))
      }
      joinNode     <- wrapInAliased(rightNode)
      aliasContext <- getContext
    } yield {
      // Join condition can refer to columns from both mainNode and joinNode,
      // as well as from outer query
      val references       = getColumnReferencesMap(mainNode) ++ getColumnReferencesMap(joinNode) ++ outerReferences
      val updatedCondition = plan.condition.map(updateExpression(_, references, aliasContext))

      mainNode.addJoin(joinNode, plan.joinType, updatedCondition)
    }
  }

}
