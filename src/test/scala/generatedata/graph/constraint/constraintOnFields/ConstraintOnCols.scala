package generatedata.graph.constraint.constraintOnFields

import generatedata.graph.OurWrapper
import generatedata.graph.constraint.Constraint
import org.apache.spark.sql.catalyst.expressions.NamedExpression

abstract class ConstraintOnCols extends Constraint {
  def leftCol: NamedExpression
  def rightCol: NamedExpression
  def constraintType: OurWrapper
}
