package ru.sberbank.bigdata.enki.ctl

import ru.sberbank.bigdata.enki.framework.{DerivedView, WorkflowExecutable, WorkflowTask}
import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.edge.LDiEdge

package object dependencies {

  private[dependencies] def filterExecutable(views: Seq[WorkflowTask[_]]): Seq[WorkflowExecutable[_]] =
    views.collect { case executable: WorkflowExecutable[_] =>
      executable
    }

  /** Returns dependencies of Views/CustomActions belonging to same project, including views/custom actions themselves */
  def innerDependencies(derivedViews: Seq[WorkflowExecutable[_]]): Seq[WorkflowExecutable[_]] =
    Linearizer(derivedViews).collect { case (view, Inner) =>
      view
    }

  /** Returns dependencies of View/CustomAction belonging to same project, including view/custom action itself */
  def innerDependencies(derivedView: WorkflowExecutable[_]): Seq[WorkflowExecutable[_]] =
    Linearizer(Seq(derivedView)).collect { case (view, Inner) =>
      view
    }

  /** Returns dependencies of Views/CustomActions belonging to other projects, excluding views/custom actions themselves */
  def outerDependencies(derivedViews: Seq[WorkflowExecutable[_]]): Vector[WorkflowExecutable[_]] =
    Linearizer(derivedViews).collect { case (view, Outer) =>
      view
    }

  /** Returns dependencies of View/CustomAction belonging to other projects, excluding view/custom action itself */
  def outerDependencies(derivedView: WorkflowExecutable[_]): Seq[WorkflowExecutable[_]] =
    Linearizer(Seq(derivedView)).collect { case (view, Outer) =>
      view
    }

  /** Returns dependencies of Views/CustomActions that are Sources */
  def sourceDependencies(derivedViews: Seq[WorkflowExecutable[_]]): Set[WorkflowTask[_]] =
    sourceEdges(derivedViews).toSet

  private def sourceEdges(derivedViews: Seq[WorkflowExecutable[_]]): Iterable[WorkflowTask[_]] =
    LabeledDirectedGraph(derivedViews).edges
      .filter((edge: LDiEdge[WorkflowTask[_]]) => edge.label == Source) // filter only 'Source' dependencies
      .map(_.source)                                                    // take source ("left") node of a directed edge

  def genGraph(derivedViews: Seq[WorkflowExecutable[_]]): Graph[WorkflowTask[_], DiEdge] = DirectedGraph(derivedViews)
  def genGraph(derivedView: WorkflowExecutable[_]): Graph[WorkflowTask[_], DiEdge]       = DirectedGraph(Seq(derivedView))

  def genPersistedGraph(derivedViews: Seq[WorkflowExecutable[_]]): Graph[WorkflowTask[_], DiEdge] =
    DirectedGraph(derivedViews, persistOnly = true)

  def genPersistedGraph(derivedView: DerivedView[_]): Graph[WorkflowTask[_], DiEdge] =
    DirectedGraph(Seq(derivedView), persistOnly = true)

  def genLabeledGraph(derivedViews: Seq[WorkflowExecutable[_]]): Graph[WorkflowTask[_], LDiEdge] =
    LabeledDirectedGraph(derivedViews)

  def genLabeledGraph(derivedView: WorkflowExecutable[_]): Graph[WorkflowTask[_], LDiEdge] =
    LabeledDirectedGraph(Seq(derivedView))

  private[ctl] type DependencyLayer = Seq[WorkflowExecutable[_]]

  private[ctl] def innerDependenciesLayers(derivedViews: Seq[WorkflowExecutable[_]]): Seq[DependencyLayer] =
    genGraph(derivedViews).topologicalSort
      .fold(cycleNode => throw new Exception(s"Circular dependency found at ${cycleNode.value}"), _.toLayered)
      .map { case (_, nodes) =>
        nodes
          .map(_.value)
          .collect { case d: WorkflowExecutable[_] => d }
          .toSeq
      }
      .toSeq
      .filter(_.nonEmpty)

  final private[ctl] val PdfWidth = 700

}
