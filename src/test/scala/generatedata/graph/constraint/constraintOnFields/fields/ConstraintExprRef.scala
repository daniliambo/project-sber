package generatedata.graph.constraint.constraintOnFields.fields

import generatedata.graph.OurWrapper
import generatedata.graph.constraint.constraintOnFields.ConstraintOnCols
import org.apache.spark.sql.catalyst.expressions.AttributeReference

final case class ConstraintExprRef(leftCol: AttributeReference,
                                   rightCol: AttributeReference,
                                   constraintType: OurWrapper
) extends ConstraintOnCols
