package generatedata.generators.params

import generatedata.graph.constraint.Constraint
import org.apache.spark.sql.types.StructType
import ru.sberbank.bigdata.enki.plan.columns.Column
import ru.sberbank.bigdata.enki.plan.nodes.Node

abstract class Params {
  def columns: Vector[Column]
  def schema: StructType
  def constr: Constraint
}
