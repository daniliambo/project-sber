package generatedata.graph.constraint.constraintOnTables

import generatedata.graph.OurWrapper
import generatedata.graph.constraint.Constraint
import generatedata.utils.SetConstraint
import org.apache.spark.sql.catalyst.expressions.NamedExpression

final case class ConstraintFilter(constraints: SetConstraint) extends Constraint

