package generatedata

import generatedata.graph.constraint.Constraint
import generatedata.graph.constraint.constraintOnFields.ConstraintOnCols
import ru.sberbank.bigdata.enki.framework.{Table, WorkflowTask}
import ru.sberbank.bigdata.enki.plan.nodes.Node

package object utils {

  type TableType[T] = Table[T] with WorkflowTask[T]
  type SetConstraint = Set[ConstraintOnCols]

}
