package ru.sberbank.bigdata.enki.plan.converter.transformers

import cats.syntax.applicative._
import cats.syntax.option._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, Project}
import org.apache.spark.sql.types.ByteType
import ru.sberbank.bigdata.enki.plan.columns.Column
import ru.sberbank.bigdata.enki.plan.expression.{AttributeFromColumn, GroupingExpression, GroupingIdExpression}
import ru.sberbank.bigdata.enki.plan.nodes._
import ru.sberbank.bigdata.enki.plan.util.NameContext

import scala.annotation.tailrec

private[converter] object AggregateTransformer extends PlanTransformer[Aggregate] {

  override def transform(plan: Aggregate, children: List[Node], outerReferences: ReferenceMap): ContextState[Node] = {
    val Aggregate(groupingExprs, aggregateExprs, childPlan) = plan
    val childNode                                           = children.head

    (childNode, childPlan) match {
      // CUBE, ROLLUP, GROUPING SETS case
      // For more info check org.apache.spark.sql.catalyst.analysis.ResolveGroupingAnalytics
      case (selectNode: SelectNode, Expand(projections, _, Project(projectList, _))) =>
        val groupByAttrs = groupingExprs.asInstanceOf[Seq[Attribute]] // Cast is safe
        groupingAnalytics(groupByAttrs, aggregateExprs, projections, projectList, outerReferences, selectNode)

      case (otherNode, _) =>
        for {
          selectNode <- otherNode match {
            case selectNode @ SelectNode(_, _, true, _, None, None, None, None, false) =>
              selectNode.pure[ContextState]

            case _ => wrapInSelect(otherNode)
          }
          aliasContext <- getContext
        } yield {
          val references = getColumnReferencesMap(selectNode.source) ++ outerReferences

          // We need to rename added by Spark aggregate expressions from HAVING clause before converting
          val renamedAggregateExprs = renameGeneratedExpressions(aggregateExprs.toVector)
          val aggregateColumns      = convertNamedExpressions(renamedAggregateExprs, references, aliasContext)

          if (groupingExprs.isEmpty) { // Cases like "SELECT max(column) FROM table"
            selectNode.withColumns(aggregateColumns)
          } else {
            val updatedGroupingExpressions = groupingExprs
              .map(updateExpression(_, references, aliasContext))
              .toVector

            selectNode
              .withColumns(aggregateColumns)
              .withGroupByClause(updatedGroupingExpressions, GroupType.GroupByType)
          }
        }
    }
  }

  private def groupingAnalytics(
    groupByAttrs: Seq[Attribute],
    aggregateExprs: Seq[NamedExpression],
    projections: Seq[Seq[Expression]],
    projectList: Seq[NamedExpression],
    outerReferences: ReferenceMap,
    childNode: SelectNode
  ): ContextState[Node] = {
    val groupByLength = groupByAttrs.length - 1 // gid is not counted
    val gid           = groupByAttrs.last

    // Spark turns original group by expressions into aliases and appends them to the end of the projectList
    val (projectExprs, groupByAliases) = projectList.splitAt(projectList.length - groupByLength)

    val groupByIds           = groupByAliases.map(_.exprId.id)
    val filteredChildColumns = childNode.columns.filterNot(column => groupByIds contains column.id)
    val updatedChild         = childNode.withColumns(filteredChildColumns)

    for {
      aliasedNode  <- wrapInAliased(updatedChild)
      aliasContext <- getContext
    } yield {
      val references = getColumnReferencesMap(aliasedNode) ++ outerReferences

      // Because of CombineProjects rule Spark can insert expressions from Project
      // inside original grouping expressions in Aggregate
      val replaceProjectExprs = projectExprs.collect { case alias: Alias => alias.child -> alias.toAttribute }.toMap
      val originalGroupByExpr = groupByAliases.map { case Alias(child, _) =>
        val rawExpr = child.transform {
          case expr if replaceProjectExprs.contains(expr) => replaceProjectExprs(expr)
        }
        updateExpression(rawExpr, references, aliasContext)
      }.toVector

      // Take only group by expressions
      val truncatedProjections = projections.map(_.takeRight(groupByLength + 1))
      val groupByType          = groupingType(originalGroupByExpr, truncatedProjections)

      val replaceGroupByAttrs = groupByAttrs.zip(originalGroupByExpr).toMap

      val selectNode = SelectNode(aliasedNode)
      val aggregateColumns = convertAggregateExpressions(
        aggregateExprs,
        originalGroupByExpr,
        replaceGroupByAttrs,
        gid,
        references,
        aliasContext
      )
      selectNode
        .withColumns(aggregateColumns)
        .withGroupByClause(originalGroupByExpr, groupByType)
    }
  }

  private def groupingType(groupByExpr: Seq[Expression], projections: Seq[Seq[Expression]]): GroupType = {
    val groupingIds = projections.map { projection =>
      val gid = projection.last
      gid match {
        case IntegerLiteral(value) => value
      }
    }.toVector

    if (isCube(groupingIds)) {
      GroupType.Cube
    } else if (isRollup(groupingIds)) {
      GroupType.Rollup
    } else {
      makeGroupingSets(groupByExpr, projections)
    }
  }

  // Checks if the sequence of numbers was generated for CUBE
  private def isCube(groupingIds: Vector[Int]): Boolean = {
    @tailrec
    def go(idx: Int): Boolean =
      if (idx >= groupingIds.length) {
        true
      } else if (idx != groupingIds(idx)) {
        false
      } else {
        go(idx + 1)
      }

    go(0)
  }

  // Checks if the sequence of numbers was generated for ROLLUP
  private def isRollup(groupingIds: Vector[Int]): Boolean = {
    @tailrec
    def go(idx: Int, lastId: Int): Boolean =
      if (idx >= groupingIds.length) {
        true
      } else if (lastId != groupingIds(idx)) {
        false
      } else {
        val newId = lastId | (1 << idx)
        go(idx + 1, newId)
      }

    go(0, 0)
  }

  private def makeGroupingSets(groupByExprs: Seq[Expression], sets: Seq[Seq[Expression]]): GroupType.GroupingSets = {
    val groupingSetsExprs = sets.map { set =>
      set.zip(groupByExprs).flatMap { case (setExpr, groupByExpr) =>
        setExpr match {
          case Literal(null, _) => None
          case _                => groupByExpr.some
        }
      }
    }

    GroupType.GroupingSets(groupingSetsExprs)
  }

  private def convertAggregateExpressions(
    expressions: Seq[NamedExpression],
    groupByExprs: Seq[Expression],
    replaceGroupByAttrs: Map[Attribute, Expression],
    gid: Attribute,
    references: ReferenceMap,
    aliasContext: NameContext
  ): Vector[Column] = {
    val renamedAggregateExprs = renameGeneratedExpressions(expressions.toVector)

    renamedAggregateExprs.map {
      case attribute: Attribute => // This attribute was created by Spark from aliased one
        val originalAttr = replaceGroupByAttrs(attribute).asInstanceOf[AttributeFromColumn]
        val column       = references(originalAttr.exprId.id)
        column.withId(attribute.exprId.id)

      case alias: Alias =>
        val updatedChild = alias.child.transform {
          // Replace GroupingID expression with GroupingIdExpression
          case `gid` => GroupingIdExpression

          // Replace Grouping(expr) with GroupingExpression
          case Cast(BitwiseAnd(ShiftRight(`gid`, IntegerLiteral(idx)), IntegerLiteral(1)), ByteType, _) =>
            val expr = groupByExprs(groupByExprs.size - idx - 1)
            GroupingExpression(expr)

          // Replace created by Spark attribute with original grouping expression
          case attr: Attribute if replaceGroupByAttrs.contains(attr) => replaceGroupByAttrs(attr)
        }

        val updatedAlias = alias.copy(
          child = updatedChild
        )(
          exprId           = alias.exprId,
          qualifier        = alias.qualifier,
          explicitMetadata = alias.explicitMetadata
        )

        convertNamedExpression(updatedAlias, references, aliasContext)
    }
  }

}
