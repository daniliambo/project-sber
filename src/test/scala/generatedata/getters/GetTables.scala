package generatedata.getters

import generatedata.generators.DataGenerators
import generatedata.unittest.PPrint
import generatedata.writetables.GenerateTables
import generatedata.generators.params.{paramsFilterJoin, GlobalParams, SetOpParams}
import generatedata.generators.params.paramsFilterJoin.param.{FilterParams, JoinParams}
import generatedata.graph.constraint._
import generatedata.graph.constraint.constraintOnTables._
import generatedata.graph.constraint.constraintOnFields._
import generatedata.graph.constraint.constraintOnFields.fields.{ConstraintExprCol, ConstraintExprRef}
import generatedata.graph.{BuildGraphMethods, GetConstraints, OurWrapper}
import generatedata.modules.module1.S3
import generatedata.test.{DFS, ReadTable, WriteTable}
import generatedata.utils.{GenerateTypeFunctions, GeneratorConfig, TableType}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec
import ru.sberbank.bigdata.enki.ctl.dependencies.{innerDependencies, outerDependencies, sourceDependencies}
import ru.sberbank.bigdata.enki.plan.nodes.{getColumnReferences, Node, SourceTableNode}
import ru.sberbank.bigdata.enki.test.SparkTest
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import ru.sberbank.bigdata.enki.framework.DerivedTable
import ru.sberbank.bigdata.enki.plan.columns.{Column, SourceColumn}
import ru.sberbank.bigdata.enki.plan.expression.AttributeFromColumn

import scala.collection.mutable

trait GetTables
  extends GenerateTypeFunctions
    with WriteTable
    with ReadTable
    with DataGenerators
    with DFS
    with GetTablesMethods
    with BuildGraphMethods
    with GetConstraints
    with SparkTest
    with GenerateTables
    with PPrint {

  def main(Step: DerivedTable[Row]): mutable.Map[SourceTableNode, DataFrame] = {
    val sortedGraph = setUp(Step)

    val mapOfAllConstraints: mutable.Map[SourceTableNode, mutable.Set[(Node, Constraint)]] = buildGraph(sortedGraph)

    //init passedParams
    val mapOfGeneratedConstraintsData = mutable.Map.empty[(Node, Constraint), DataFrame]
    val n = 20
    val generatorConfig = GeneratorConfig()

    implicit val globalParams: GlobalParams = GlobalParams(mapOfGeneratedConstraintsData, n, generatorConfig)

    val mapOfGeneratedData: mutable.Map[SourceTableNode, DataFrame] = mapOfAllConstraints.map {
      case (sourceTable, constraintsSet) =>
        val schema: StructType = StructType(
          sourceTable.columns.map(x => (x.name, x.dataType, x.nullable)).map(x => StructField(x._1, x._2, x._3))
        )

        sourceTable ->
          generateData(sourceTable, constraintsSet, schema)
    }
    mapOfGeneratedData
  }

  def setUp(Step: DerivedTable[Row]): Seq[DependencyLayer] = {
    val leaves = Seq(Step)
    val steps = getAllSteps(leaves)

    val sourceDeps =
      (sourceDependencies(leaves) ++ innerDependencies(leaves) ++ outerDependencies(leaves)).collect({
        case t: TableType[_] => t
      })

    //generate Data
    generateSchemesAndTables(sourceDeps)

    val sortedGraph = innerDependenciesLayers(steps)

    sortedGraph
  }

}
