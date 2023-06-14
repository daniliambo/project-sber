package ru.sberbank.bigdata.enki.plan.converter.transformers

import cats.syntax.applicative._
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project}
import ru.sberbank.bigdata.enki.plan.nodes._

private[converter] object ProjectTransformer extends PlanTransformer[Project] {

  override def transform(plan: Project, children: List[Node], outerReferences: ReferenceMap): ContextState[Node] =
    for {
      selectNode <- (children.head, plan.child) match {
        // SELECT without FROM clause
        case (emptyNode, OneRowRelation) => SelectNode(emptyNode).pure[ContextState]

        case (selectNode @ SelectNode(_, _, true, _, None, None, None, None, false), _) =>
          selectNode.pure[ContextState]

        case (otherNode, _) => wrapInSelect(otherNode)
      }
      aliasContext <- getContext
    } yield {
      val references = getColumnReferencesMap(selectNode.source) ++ outerReferences
      val columns    = convertNamedExpressions(plan.projectList, references, aliasContext)

      selectNode.withColumns(columns)
    }

}
