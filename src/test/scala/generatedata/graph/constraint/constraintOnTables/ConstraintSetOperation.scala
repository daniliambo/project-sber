package generatedata.graph.constraint.constraintOnTables

import generatedata.graph.OurWrapper
import generatedata.graph.constraint.Constraint
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import ru.sberbank.bigdata.enki.plan.columns.Column

final case class ConstraintSetOperation(columns: Vector[Column]) extends Constraint