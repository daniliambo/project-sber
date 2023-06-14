package ru.sberbank.bigdata.enki.plan.converter.transformers

import cats.syntax.applicative._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import ru.sberbank.bigdata.enki.plan.columns.{InlineColumn, SourceColumn}
import ru.sberbank.bigdata.enki.plan.nodes.{InlineTableNode, Node, ReferenceMap, SourceTableNode}

object LogicalRelationTransformer extends PlanTransformer[LogicalRelation] {

  override def transform(
    plan: LogicalRelation,
    children: List[Node],
    outerReferences: ReferenceMap
  ): ContextState[Node] = {
    val LogicalRelation(_, output, catalogTable) = plan

    val node: Node = catalogTable match {
      case Some(table) if table.identifier.database.isDefined =>
        val schema  = table.database
        val name    = table.identifier.table
        val columns = output.map(SourceColumn(_)).toVector
        SourceTableNode(schema, name, columns)

      case _ => // When this case happens?
        val columns = output.map(InlineColumn(_)).toVector
        InlineTableNode(columns, Vector.empty)
    }

    node.pure[ContextState]
  }

}
