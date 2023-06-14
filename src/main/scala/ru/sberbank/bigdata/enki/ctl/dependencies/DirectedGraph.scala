package ru.sberbank.bigdata.enki.ctl.dependencies

import ru.sberbank.bigdata.enki.framework.{WorkflowExecutable, WorkflowTask}
import scalax.collection.Graph
import scalax.collection.GraphEdge.DiEdge
import scalax.collection.GraphPredef._

import scala.collection.mutable

/** Directed graph used for inner-project dependencies construction
  */
private[dependencies] object DirectedGraph {

  def apply(tasks: Seq[WorkflowExecutable[_]], persistOnly: Boolean = false): Graph[WorkflowTask[_], DiEdge] = {

    val projects: Seq[String] = tasks.map(_.project)

    val toNodes = new mutable.ArrayBuffer[WorkflowTask[_]]

    val graph = scalax.collection.mutable.Graph.empty[WorkflowTask[_], DiEdge]

    def go(tasks: Seq[WorkflowExecutable[_]]): Unit =
      for (task <- tasks if !toNodes.contains(task)) {
        toNodes += task
        if (projects.contains(task.project)) {

          val derivedDependencies = filterExecutable(task.dependencies)

          if (!persistOnly || task.generateWfStep) {
            graph += task

            derivedDependencies
              .filter(d => projects.contains(d.project) && (d.generateWfStep || !persistOnly))
              .foreach(d => graph += (d ~> task))
          }

          go(derivedDependencies)
        }
      }

    go(tasks)

    graph
  }
}
