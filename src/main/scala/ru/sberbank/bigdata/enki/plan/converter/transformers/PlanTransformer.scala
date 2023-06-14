package ru.sberbank.bigdata.enki.plan.converter.transformers

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import ru.sberbank.bigdata.enki.plan.nodes.{Node, ReferenceMap}

private[converter] trait PlanTransformer[T <: LogicalPlan] {

  /** Transforms given [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan LogicalPlan]]
    * into [[ru.sberbank.bigdata.enki.plan.nodes.Node Node]]
    *
    * @param plan            LogicalPlan to transform
    * @param children        already transformed children of the plan
    * @param outerReferences columns from the outer query
    *                        (used in [[org.apache.spark.sql.catalyst.expressions.SubqueryExpression SubqueryExpression]])
    */
  def transform(plan: T, children: List[Node], outerReferences: ReferenceMap): ContextState[Node]

}
