package generatedata.generators.params.paramsFilterJoin.param

import generatedata.generators.params.paramsFilterJoin.ColsParams
import generatedata.graph.constraint.Constraint
import generatedata.graph.constraint.constraintOnFields.ConstraintOnCols
import generatedata.graph.constraint.constraintOnTables.ConstraintFilter
import org.apache.spark.sql.types.StructType
import ru.sberbank.bigdata.enki.plan.columns.Column
import ru.sberbank.bigdata.enki.plan.nodes.Node

final case class FilterParams(constr: ConstraintFilter,
                              schema: StructType
) extends ColsParams {
  override def columns: Vector[Column] = ???
}
