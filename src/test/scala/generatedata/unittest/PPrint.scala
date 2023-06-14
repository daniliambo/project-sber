package generatedata.unittest

import generatedata.graph.constraint.{Constraint, ConstraintEmpty}
import generatedata.graph.constraint.constraintOnTables.{ConstraintJoin, ConstraintSetOperation}
import ru.sberbank.bigdata.enki.plan.columns.Column
import ru.sberbank.bigdata.enki.plan.nodes.{Node, SourceTableNode}

import scala.collection.mutable

trait PPrint {

  def printConstraints(constraints: mutable.Map[SourceTableNode, Seq[(Constraint, Node)]], depth: Int = 0): Unit =
    constraints.foreach { case (sourceNode, seq) =>
      println(sourceNode.fullName + "\n")
      println(sourceNode.columns + "\n")
      seq.foreach { case (constraint, node) =>
        constraint match {
          case ConstraintJoin(constraints) =>
            constraints.foreach { constr: Constraint =>
              println(constr)
            }
          case ConstraintSetOperation(columns) =>
            columns.mkString
          case ConstraintEmpty =>
            println("ConstraintEmpty")
          case _ =>
            println("Other")
        }
      }
    }

}
