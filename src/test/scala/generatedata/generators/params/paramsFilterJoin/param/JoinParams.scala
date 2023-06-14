package generatedata.generators.params.paramsFilterJoin.param

import generatedata.generators.params.Params
import generatedata.graph.constraint.Constraint
import org.apache.spark.sql.types.StructType
import ru.sberbank.bigdata.enki.plan.columns.Column
import ru.sberbank.bigdata.enki.plan.nodes.Node

final case class JoinParams(constr: Constraint,
                            schema: StructType
) extends Params {
  override def columns: Vector[Column] = ???
}
