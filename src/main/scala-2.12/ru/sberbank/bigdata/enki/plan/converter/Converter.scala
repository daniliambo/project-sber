package ru.sberbank.bigdata.enki.plan.converter

import cats.data.State
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.enki.getLogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{Dataset, SparkSession}
import ru.sberbank.bigdata.enki.framework.DerivedTable
import ru.sberbank.bigdata.enki.plan.converter.transformers._
import ru.sberbank.bigdata.enki.plan.nodes._
import ru.sberbank.bigdata.enki.plan.util.NameContext

import scala.annotation.tailrec
import scala.collection.immutable.{LongMap, Queue}

object Converter {

  private val logger = Logger.getLogger(getClass)

  /** Create a [[Node]] from given [[DerivedTable]] */
  def convertTable(table: DerivedTable[_])(implicit spark: SparkSession): Node = {
    val dataset = table.gen
    convertDataset(dataset)
  }

  /** Create a [[Node]] from given [[Dataset]] */
  def convertDataset(dataset: Dataset[_]): Node = {
    val plan           = getLogicalPlan(dataset)
    val simplifiedPlan = CustomRuleExecutor.execute(plan)

    convertPlan(simplifiedPlan, LongMap.empty, NameContext.empty)
  }

  private[converter] def convertPlan(plan: LogicalPlan,
                                     outerReferences: ReferenceMap,
                                     aliasContext: NameContext
  ): Node = {
    logger.info(
      s"""
         |Starting conversion from Logical Plan to Node. Given Logical Plan:
         |$plan
         |""".stripMargin
    )

    val (numerated, parentsMap) = walkDown(plan)
    val node                    = walkUp(numerated, parentsMap, outerReferences, aliasContext)

    logger.info("Successfully converted Logical Plan into Node")
    node match {
      case _: SelectNode | _: SetOperationNode => node
      case other                               => SelectNode(other)
    }
  }

  private type PlanWithIndex = (Long, LogicalPlan)

  private val firstAncestorIdx = -1L

  /** Safely traverses given [[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]]
    * and returns the list of its numerated vertices with edges
    */
  private def walkDown(root: LogicalPlan): (List[PlanWithIndex], LongMap[Long]) = {
    @tailrec def go(
      queue: Queue[PlanWithIndex],
      currentIdx: Long
    )(
      plans: List[PlanWithIndex],
      edges: List[(Long, Long)]
    ): (List[PlanWithIndex], LongMap[Long]) =
      if (queue.isEmpty) {
        (plans, LongMap(edges: _*))
      } else {
        val ((parentIdx, plan), rest) = queue.dequeue
        val transformedPlan           = transformPlan(plan)

        val updatedEdges = (currentIdx, parentIdx) :: edges
        val updatedPlans = (currentIdx, transformedPlan) :: plans

        val children = transformedPlan.children.map(currentIdx -> _).toList

        go(rest.enqueue(children), currentIdx + 1)(updatedPlans, updatedEdges)
      }

    go(Queue(firstAncestorIdx -> root), 0)(Nil, Nil)
  }

  /** Applies the following transformation to the plan:
    *
    *   1. Replaces a chain of [[org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias]]es with a single
    *      [[org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias]] using an alias from the first plan
    *
    *   1. Replaces a `GlobalLimit(limitExpr, LocalLimit(_, child))` with a `GlobalLimit(limitExpr, child)`
    *
    *   1. Replaces a `DropDuplicates(keys, child, ...)` with a `Distinct(child)` if `keys` and `child.output`
    *      contain the same elements
    *
    *   1. Removes [[org.apache.spark.sql.catalyst.plans.logical.ResolvedHint]]
    */
  @tailrec private def transformPlan(plan: LogicalPlan): LogicalPlan =
    plan match {
      case SubqueryAlias(alias, SubqueryAlias(_, child)) =>
        transformPlan(SubqueryAlias(alias, child))

      case GlobalLimit(limit, LocalLimit(_, child)) =>
        transformPlan(GlobalLimit(limit, child))

      case Deduplicate(keys, child) if keys.toSet == child.output.toSet =>
        Distinct(child)

      case ResolvedHint(child, _) => transformPlan(child)

      case _ => plan
    }

  private def walkUp(
    numerated: List[PlanWithIndex],
    parentMap: LongMap[Long],
    outerReferences: ReferenceMap,
    aliasContext: NameContext
  ): Node = {
    @tailrec def go(stack: List[PlanWithIndex], childrenMap: LongMap[List[Node]], aliasContext: NameContext): Node =
      stack match {
        case (idx, plan) :: rest =>
          val planChildren              = childrenMap.getOrElse(idx, Nil)
          val (updatedContext, s2TNode) = fromPlan(plan, planChildren, outerReferences).run(aliasContext).value

          val parentIdx      = parentMap(idx)
          val parentChildren = childrenMap.getOrElse(parentIdx, Nil)

          // Add created s2t node to the parent children
          val updatedChildrenMap = childrenMap.updated(parentIdx, s2TNode :: parentChildren)

          go(rest, updatedChildrenMap, updatedContext)

        case Nil => childrenMap(firstAncestorIdx).head
      }

    go(numerated, LongMap.empty, aliasContext)
  }

  private def fromPlan(plan: LogicalPlan, children: List[Node], outerReferences: ReferenceMap): ContextState[Node] =
    plan match {
      case logicalRelation: LogicalRelation =>
        LogicalRelationTransformer.transform(logicalRelation, children, outerReferences)

      case oneRowRelation: OneRowRelation =>
        OneRowRelationTransformer.transform(oneRowRelation, children, outerReferences)

      case localRelation: LocalRelation =>
        LocalRelationTransformer.transform(localRelation, children, outerReferences)

      case subqueryAlias: SubqueryAlias =>
        SubqueryAliasTransformer.transform(subqueryAlias, children, outerReferences)

      case union: Union =>
        SetOperationTransformer.transform(union, children, outerReferences)

      case setOperation: SetOperation =>
        SetOperationTransformer.transform(setOperation, children, outerReferences)

      case join: Join =>
        JoinTransformer.transform(join, children, outerReferences)

      case generate: Generate =>
        GenerateTransformer.transform(generate, children, outerReferences)

      case sort: Sort =>
        SortTransformer.transform(sort, children, outerReferences)

      case distinct: Distinct =>
        DistinctTransformer.transform(distinct, children, outerReferences)

      case globalLimit: GlobalLimit =>
        GlobalLimitTransformer.transform(globalLimit, children, outerReferences)

      case filter: Filter =>
        FilterTransformer.transform(filter, children, outerReferences)

      case window: Window =>
        WindowTransformer.transform(window, children, outerReferences)

      case aggregate: Aggregate =>
        AggregateTransformer.transform(aggregate, children, outerReferences)

      case project: Project =>
        ProjectTransformer.transform(project, children, outerReferences)

      case _: Expand => // Skip Expand 'cause it will be checked as a part of Aggregate
        State.pure(children.head)

      case other =>
        val error = new IllegalArgumentException(s"Currently ${other.nodeName} is not supported")
        logger.error(error.getMessage, error)
        throw error
    }

  // Used to simplify the analyzed plan generated by Spark
  private object CustomRuleExecutor extends RuleExecutor[LogicalPlan] {

    private val rules =
      List(CombineUnions, CollapseProject, CollapseWindow, CombineFilters, SimplifyCasts, RemoveRedundantProject)

    override protected def batches: Seq[Batch] = Seq(Batch("Simplify plan", Once, rules: _*))
  }

}
