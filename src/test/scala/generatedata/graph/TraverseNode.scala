package generatedata.graph

import com.typesafe.scalalogging.LazyLogging
import generatedata.graph.constraint.Constraint
import generatedata.graph.constraint.constraintOnTables.{ConstraintFilter, ConstraintJoin, ConstraintSetOperation}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, PredicateHelper}
import ru.sberbank.bigdata.enki.plan.nodes._

import scala.collection.mutable

trait TraverseNode extends PredicateHelper with UnfoldExpression with LazyLogging {
  //init

  def traverseNode(node: Node,
                   constraintsPassDown: mutable.Set[(Node, Constraint)]
                  ): mutable.Map[SourceTableNode, mutable.Set[(Node, Constraint)]] = {
    val mapOfConstraints = mutable.Map.empty[SourceTableNode, mutable.Set[(Node, Constraint)]]

    //fill mapOfConstraints
    dfsNode(Set(node), constraintsPassDown, mapOfConstraints)
    mapOfConstraints

  }

  def dfsNode(nodes: Set[Node],
              constraintsPassDown: mutable.Set[(Node, Constraint)],
              mapOfConstraints: mutable.Map[SourceTableNode, mutable.Set[(Node, Constraint)]]
             ): Unit =
    for (node <- nodes) node match {
      case node: LeafNode =>
        node match {
          // generate tables with given constraints
          case table: SourceTableNode =>
            mapOfConstraints.update(table, constraintsPassDown)
        }

      //Intersect, Except, Union
      case node: SetOperationNode =>
        val constraint = ConstraintSetOperation(node.columns)
        logger.info(s"SetOperationNode constraints from unfoldExpression: $constraint")

        dfsNode(node.children.toSet, constraintsPassDown + (constraint -> node), mapOfConstraints)

      // process JoinNode
      case JoinNode(mainNode, joins) =>
        val constraintsForMainNode = mutable.Set.empty[(Node, Constraint)]
        constraintsForMainNode ++= constraintsPassDown
        for (join <- joins) {
          val constraints = unfoldExpression(join.condition)
          val constr = ConstraintJoin(constraints)
          logger.info(s"JoinNode constraints from unfoldExpression: $constraints")
          dfsNode(join.node.children.toSet, constraintsPassDown + (constr -> node), mapOfConstraints)
          constraintsForMainNode += (constr -> node)

        }
        dfsNode(mainNode.children.toSet, constraintsForMainNode, mapOfConstraints)

      // process SelectNode
      case filter: SelectNode =>
        val constraints = unfoldExpression(filter.whereClause)
        val constr = ConstraintFilter(constraints)

        logger.info(s"SelectNode constraint from unfoldExpression: $constraints")
        dfsNode(node.children.toSet, constraintsPassDown + (constr -> node), mapOfConstraints)

      // process AliasedNode
      case aliasedNode: AliasedNode =>
        logger.info(s"AliasedNode constraintsPassDown: $constraintsPassDown")
        dfsNode(node.children.toSet, constraintsPassDown, mapOfConstraints)
    }
}
