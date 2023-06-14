package generatedata.graph

import ru.sberbank.bigdata.enki.ctl.dependencies.genGraph
import ru.sberbank.bigdata.enki.framework.{DerivedTable, WorkflowExecutable}

trait BuildGraphMethods {

  type DependencyLayer = Seq[DerivedTable[_]]

  def innerDependenciesLayers(derivedViews: Seq[WorkflowExecutable[_]]): Seq[DependencyLayer] =
    genGraph(derivedViews).topologicalSort
      .fold(cycleNode => throw new Exception(s"Circular dependency found at ${cycleNode.value}"), _.toLayered)
      .map { case (_, nodes) =>
        nodes
          .map(_.value)
          .collect { case d: DerivedTable[_] => d }
          .toSeq
      }
      .toSeq
      .filter(_.nonEmpty)
}
