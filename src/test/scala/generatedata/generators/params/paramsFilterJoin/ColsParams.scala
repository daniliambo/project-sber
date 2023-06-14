package generatedata.generators.params.paramsFilterJoin

import generatedata.generators.params.Params
import generatedata.graph.constraint.Constraint
import org.apache.spark.sql.types.StructType
import ru.sberbank.bigdata.enki.plan.columns.Column
import ru.sberbank.bigdata.enki.plan.nodes.Node

abstract class ColsParams extends Params {
  override def schema: StructType

  override def constr: Constraint

}
