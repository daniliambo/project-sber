package ru.sberbank.bigdata.enki.ctl.dependencies

import ru.sberbank.bigdata.enki.framework.WorkflowExecutable

import scala.collection.mutable

private[dependencies] object Linearizer {

  private[dependencies] def apply(
    tasks: Seq[WorkflowExecutable[_]]
  ): Vector[(WorkflowExecutable[_], DependencyType)] = {

    val projects = tasks.map(_.project).toSet

    val marked = new mutable.ArrayBuffer[WorkflowExecutable[_]]

    val dependencies = new mutable.ArrayBuffer[(WorkflowExecutable[_], DependencyType)]

    def go(tasks: Seq[WorkflowExecutable[_]]): Unit =
      tasks.foreach { task =>
        if (!dependencies.map(_._1).contains(task)) {
          if (task.generateWfStep && marked.contains(task)) {
            throw new Exception(s"Cyclic dependencies detected at $task.")
          } else {
            marked += task

            if (projects.contains(task.project)) {
              go(filterExecutable(task.dependencies))

              if (task.generateWfStep) dependencies += ((task, Inner))

            } else {

              if (task.generateWfStep) dependencies += ((task, Outer))

            }
          }
        }
      }

    go(tasks)

    dependencies.toVector
  }

}
