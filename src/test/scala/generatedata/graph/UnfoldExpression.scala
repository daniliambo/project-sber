package generatedata.graph

import com.typesafe.scalalogging.LazyLogging
import generatedata.graph.constraint.constraintOnFields.{ConstraintOnCols, fields}
import generatedata.graph.constraint.constraintOnFields.fields.{ConstraintExprCol, ConstraintExprRef}
import generatedata.graph.constraint.constraintOnTables.ConstraintJoin
import generatedata.graph.constraint.{Constraint, ConstraintEmpty, constraintOnFields}
import generatedata.utils.SetConstraint
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, PredicateHelper}
import ru.sberbank.bigdata.enki.plan.expression.AttributeFromColumn

trait UnfoldExpression extends PredicateHelper with LazyLogging {

  // внутри expression в зависимости от типа expression лежит leftCol и rightCol
  // does it work the same way both for join and filter?
  def unfoldExpression(conditions: Option[Expression]): Set[ConstraintOnCols] = {

    val exprs = conditions match {
      case Some(conds) => splitConjunctivePredicates(conds)
      case _           => Seq()
    }

    logger.info(s"exprs from unfoldExpression: ${exprs.mkString}")

    val constraints = exprs.map {
      case eq @ EqualTo(leftCol: AttributeReference, rightCol: AttributeReference) =>
        fields.ConstraintExprRef(leftCol, rightCol, OurWrapper(eq))
      case eq @ EqualNullSafe(leftCol: AttributeReference, rightCol: AttributeReference) =>
        fields.ConstraintExprRef(leftCol, rightCol, OurWrapper(eq))
      case eq @ GreaterThan(leftCol: AttributeReference, rightCol: AttributeReference) =>
        fields.ConstraintExprRef(leftCol, rightCol, OurWrapper(eq))
      case eq @ GreaterThanOrEqual(leftCol: AttributeReference, rightCol: AttributeReference) =>
        fields.ConstraintExprRef(leftCol, rightCol, OurWrapper(eq))
      case eq @ LessThanOrEqual(leftCol: AttributeReference, rightCol: AttributeReference) =>
        fields.ConstraintExprRef(leftCol, rightCol, OurWrapper(eq))
      case eq @ LessThan(leftCol: AttributeReference, rightCol: AttributeReference) =>
        fields.ConstraintExprRef(leftCol, rightCol, OurWrapper(eq))

      case eq @ EqualTo(leftCol: AttributeFromColumn, rightCol: AttributeFromColumn) =>
        ConstraintExprCol(leftCol, rightCol, OurWrapper(eq))
      case eq @ EqualNullSafe(leftCol: AttributeFromColumn, rightCol: AttributeFromColumn) =>
        fields.ConstraintExprCol(leftCol, rightCol, OurWrapper(eq))
      case eq @ GreaterThan(leftCol: AttributeFromColumn, rightCol: AttributeFromColumn) =>
        fields.ConstraintExprCol(leftCol, rightCol, OurWrapper(eq))
      case eq @ GreaterThanOrEqual(leftCol: AttributeFromColumn, rightCol: AttributeFromColumn) =>
        fields.ConstraintExprCol(leftCol, rightCol, OurWrapper(eq))
      case eq @ LessThanOrEqual(leftCol: AttributeFromColumn, rightCol: AttributeFromColumn) =>
        fields.ConstraintExprCol(leftCol, rightCol, OurWrapper(eq))
      case eq @ LessThan(leftCol: AttributeFromColumn, rightCol: AttributeFromColumn) =>
        fields.ConstraintExprCol(leftCol, rightCol, OurWrapper(eq))
      case _ => ConstraintEmpty
    }
    constraints.toSet
  }
}
