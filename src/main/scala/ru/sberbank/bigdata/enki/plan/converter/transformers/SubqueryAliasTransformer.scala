package ru.sberbank.bigdata.enki.plan.converter.transformers

import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import ru.sberbank.bigdata.enki.plan.nodes.{AliasedNode, Node, ReferenceMap}

private[converter] object SubqueryAliasTransformer extends PlanTransformer[SubqueryAlias] {

  override def transform(plan: SubqueryAlias, children: List[Node], outerReferences: ReferenceMap): ContextState[Node] =
    updateName(plan.alias).map(createAliased(children.head, _))

}
