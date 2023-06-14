package ru.sberbank.bigdata.enki.plan.converter.transformers

import cats.instances.vector._
import cats.syntax.applicative._
import cats.syntax.traverse._
import org.apache.spark.sql.catalyst.plans.logical.Window
import ru.sberbank.bigdata.enki.plan.columns.{Column, ColumnReference, ExpressionColumn}
import ru.sberbank.bigdata.enki.plan.expression.AttributeFromColumn
import ru.sberbank.bigdata.enki.plan.nodes.{getColumnReferencesMap, Node, ReferenceMap, SelectNode}
import ru.sberbank.bigdata.enki.plan.util.NameContext

private[converter] object WindowTransformer extends PlanTransformer[Window] {

  override def transform(plan: Window, children: List[Node], outerReferences: ReferenceMap): ContextState[Node] =
    for {
      selectNode <- (children.head, plan.child) match {
        case (selectNode: SelectNode, _: Window) => selectNode.pure[ContextState]

        case (selectNode: SelectNode, _) =>
          val renamedColumns = renameInsertedAttributes(selectNode.columns)
          val updatedSource  = selectNode.withColumns(renamedColumns)
          wrapInSelect(updatedSource)

        case (otherNode, _) => wrapInSelect(otherNode)
      }
      aliasContext <- getContext
    } yield {
      val references = getColumnReferencesMap(selectNode.source) ++ outerReferences

      // For nested window functions Spark assigns the names started with '_w'
      // We create more meaningful names for such expressions
      val usedNames          = selectNode.columns.map(_.name)
      val renamedExpressions = renameGeneratedExpressions(plan.windowExpressions.toVector, usedNames)

      val windowColumns = convertNamedExpressions(renamedExpressions, references, aliasContext)

      selectNode.addColumns(windowColumns)
    }

  /** In some cases, Spark Analyzer can add attributes to the child output of the
    * [[org.apache.spark.sql.catalyst.plans.logical.Window Window]]
    * if they are used later in window expressions. Sometimes it can cause name collisions.
    * For example, the following SQL:
    *
    * {{{
    *   SELECT trim(inn) AS inn,
    *          min(start_dt) OVER (PARTITION BY inn) AS first_date
    *   FROM my_schema.my_table m
    * }}}
    *
    * will be converted by Spark to the plan:
    *
    * {{{
    *   Project [inn#76, start_dt#77, end_dt#78] <- inn#79 is not present here
    *   +- Window [min(start_dt#80) windowspecdefinition(inn#79, ...) AS start_dt#77], [inn#79]
    *      +- Project [trim(inn#79) AS inn#76, start_dt#80, inn#79] <- inn#79 is inserted by Spark
    *         +- SubqueryAlias m
    *            +- Relation[inn#79,start_dt#80] parquet
    * }}}
    *
    * In such cases, these attributes need to be renamed.
    */
  private def renameInsertedAttributes(columns: Vector[Column]): Vector[Column] = {
    val expressionColumnNames = columns.collect { case ExpressionColumn(_, name, _, _) => name }.toSet

    val context = NameContext.fromNames(columns.map(_.name))
    columns.traverse {
      case columnRef: ColumnReference if expressionColumnNames.contains(columnRef.name) =>
        val attributeFromColumn = AttributeFromColumn(columnRef)

        updateName(s"old_${columnRef.name}").map { updatedName =>
          ExpressionColumn(columnRef.id, updatedName, attributeFromColumn, columnRef.sourceColumns)
        }: ContextState[Column]

      case other => other.pure[ContextState]
    }
      .runA(context)
      .value
  }

}
