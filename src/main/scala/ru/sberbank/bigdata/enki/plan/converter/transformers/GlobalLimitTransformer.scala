package ru.sberbank.bigdata.enki.plan.converter.transformers

import cats.syntax.applicative._
import org.apache.spark.sql.catalyst.plans.logical.GlobalLimit
import ru.sberbank.bigdata.enki.plan.nodes.{Node, ReferenceMap, SelectNode}

object GlobalLimitTransformer extends PlanTransformer[GlobalLimit] {

  override def transform(plan: GlobalLimit, children: List[Node], outerReferences: ReferenceMap): ContextState[Node] =
    for {
      selectNode <- children.head match {
        case selectNode: SelectNode if selectNode.limit.isEmpty =>
          selectNode.pure[ContextState]

        case otherNode => wrapInSelect(otherNode)
      }
    } yield selectNode.withLimit(plan.limitExpr)

}
