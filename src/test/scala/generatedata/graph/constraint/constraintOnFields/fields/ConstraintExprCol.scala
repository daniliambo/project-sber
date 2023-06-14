package generatedata.graph.constraint.constraintOnFields.fields

import generatedata.graph.OurWrapper
import generatedata.graph.constraint.constraintOnFields.ConstraintOnCols
import ru.sberbank.bigdata.enki.plan.expression.AttributeFromColumn

final case class ConstraintExprCol(leftCol: AttributeFromColumn,
                                   rightCol: AttributeFromColumn,
                                   constraintType: OurWrapper
) extends ConstraintOnCols
