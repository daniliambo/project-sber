package generatedata.graph.constraint

import generatedata.graph.OurWrapper
import generatedata.graph.constraint.constraintOnFields.ConstraintOnCols
import org.apache.spark.sql.catalyst.expressions.NamedExpression

case object ConstraintEmpty extends ConstraintOnCols {
  override def leftCol: NamedExpression = ???

  override def rightCol: NamedExpression = ???

  override def constraintType: OurWrapper = ???
}
