package generatedata.generators.params

import generatedata.graph.constraint.Constraint
import generatedata.utils.{GeneratorConfig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import ru.sberbank.bigdata.enki.plan.columns.Column
import ru.sberbank.bigdata.enki.plan.nodes.Node

import scala.collection.mutable

final case class SetOpParams(constr: Constraint, columns: Vector[Column], schema: StructType) extends Params
