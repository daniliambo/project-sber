package generatedata.graph.constraint.constraintOnTables

import generatedata.graph.OurWrapper
import generatedata.graph.constraint.Constraint
import generatedata.graph.constraint.constraintOnFields.ConstraintOnCols
import generatedata.utils.SetConstraint
import org.apache.spark.sql.catalyst.expressions.NamedExpression

final case class ConstraintJoin(constraints: SetConstraint) extends Constraint
