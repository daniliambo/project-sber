package ru.sberbank.bigdata.enki.ctl.dependencies

import ru.sberbank.bigdata.enki.framework.{Executable, SourceTable, WorkflowExecutable, WorkflowTask}
import scalax.collection.Graph
import scalax.collection.edge.LDiEdge

import scala.collection.mutable

object LabeledDirectedGraph {

  private[dependencies] def apply(tasks: Seq[WorkflowExecutable[_]]): Graph[WorkflowTask[_], LDiEdge] = {

    val projects = tasks.map(_.project).toSet

    val toNodes = new mutable.ArrayBuffer[WorkflowExecutable[_]]

    val graph = scalax.collection.mutable.Graph.empty[WorkflowTask[_], LDiEdge]

    implicit val factory: LDiEdge.type = scalax.collection.edge.LDiEdge // Used for graph.addLedge

    def go(tasks: Seq[WorkflowExecutable[_]]): Unit =
      tasks.foreach { task =>
        if (!toNodes.contains(task)) {
          toNodes += task
          if (projects.contains(task.project)) {
            task.dependencies.foreach {
              case s: SourceTable[_] => graph.addLEdge(s, task)(Source)
              case d: Executable[_] =>
                if (projects.contains(d.project)) graph.addLEdge(d, task)(Inner)
                else graph.addLEdge(d, task)(Outer)
              case _ =>
            }
            go(filterExecutable(task.dependencies))
          }
        }
      }

    go(tasks)

    graph
  }

}
