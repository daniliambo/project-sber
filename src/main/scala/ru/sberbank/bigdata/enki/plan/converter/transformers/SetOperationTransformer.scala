package ru.sberbank.bigdata.enki.plan.converter.transformers

import cats.syntax.applicative._
import org.apache.spark.sql.catalyst.plans.logical.{Except, Intersect, LogicalPlan, Union}
import ru.sberbank.bigdata.enki.plan.nodes._

private[converter] object SetOperationTransformer extends PlanTransformer[LogicalPlan] {

  override def transform(plan: LogicalPlan, children: List[Node], outerReferences: ReferenceMap): ContextState[Node] = {
    val node: Node = plan match {
      case _: Union =>
        val unionChildren = children.map(SelectNode(_)).toVector
        UnionNode(unionChildren)

      case _: Intersect =>
        val List(left, right) = children.map(SelectNode(_))
        IntersectNode(left, right)

      case _: Except =>
        val List(left, right) = children.map(SelectNode(_))
        ExceptNode(left, right)
    }

    node.pure[ContextState]
  }

}
