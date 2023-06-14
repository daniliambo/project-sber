package generatedata.graph

import generatedata.getters.GetTablesMethods
import generatedata.graph.constraint.Constraint
import generatedata.test.{DFS, ReadTable, WriteTable}
import generatedata.utils.{GenerateTypeFunctions, TableType}
import ru.sberbank.bigdata.enki.plan.converter.Converter
import ru.sberbank.bigdata.enki.plan.nodes.{Node, SourceTableNode}
import ru.sberbank.bigdata.enki.test.SparkTest

import scala.collection.mutable

trait GetConstraints
  extends GenerateTypeFunctions
    with WriteTable
    with ReadTable
    with SparkTest
    with DFS
    with GetTablesMethods
    with TraverseNode
    with BuildGraphMethods {

  // problem with the same Joins in different steps
  //build graph
  def buildGraph(vertices: Seq[DependencyLayer]): mutable.Map[SourceTableNode, mutable.Set[(Node, Constraint)]] = {

    val constraintsPassDown = mutable.Set.empty[(Node, Constraint)]
    val mapOfAllConstraints = mutable.Map.empty[SourceTableNode, mutable.Set[(Node, Constraint)]]

    //traverse through steps
    val flattenedVertices = vertices.flatten
    for (derivedTable <- flattenedVertices) {
      //turn derivedTable (step) into graph
      val node: Node = Converter.convertTable(derivedTable)

      //fill mapOfConstraints
      val mapOfConstraints = traverseNode(
        node: Node,
        constraintsPassDown
      )

      fillResidualConstraints(mapOfAllConstraints, mapOfConstraints)

    }
    mapOfAllConstraints
  }

  def fillResidualConstraints(mapOfAllConstraints: mutable.Map[SourceTableNode, mutable.Set[(Node, Constraint)]],
                              mapOfConstraints: mutable.Map[SourceTableNode, mutable.Set[(Node, Constraint)]]
                             ): Unit =
    mapOfConstraints.keys.foreach { k =>
      mapOfAllConstraints.get(k) match {

        case Some(constraintsall) =>
          val constraints = mapOfConstraints(k)
          mapOfAllConstraints(k) = constraints ++ constraintsall

        case None => mapOfAllConstraints.update(k, mapOfConstraints(k))
      }
    }
}
