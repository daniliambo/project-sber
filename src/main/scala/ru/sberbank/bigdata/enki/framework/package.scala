package ru.sberbank.bigdata.enki

import java.time.LocalDateTime

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.Try

package object framework {

  type WorkflowExecutable[T] = WorkflowTask[T] with Executable[T]

  def linearizeDependencies(derivedView: WorkflowExecutable[_]): Seq[WorkflowTask[_]] =
    linearizeDependencies(Seq(derivedView))

  def update(derivedViews: DerivedView[_]*)(implicit spark: SparkSession): Seq[UpdateResult] =
    update(derivedViews.toVector)

  def update(derivedViews: Vector[DerivedView[_]], changedViews: Option[Vector[WorkflowTask[_]]] = None)(
    implicit spark: SparkSession
  ): Vector[UpdateResult] = {

    spark.sparkContext.setJobGroup("enki_update", "Derived views update.")

    def go(
      views: Vector[WorkflowTask[_]],
      changed: Vector[WorkflowTask[_]],
      errors: Vector[UpdateError],
      results: Vector[UpdateResult]
    ): Vector[UpdateResult] = views match {
      case view +: remains =>
        val result = view match {
          case source: SourceView[_] =>
            SourceSkipped(source)
          case derived: DerivedView[_] =>
            if (changedViews.exists(_.contains(derived))) {
              ExternallyChanged(derived)
            } else {
              if (changedViews.isEmpty || changed.exists(derived.dependencies.contains)) {
                val errorSources = errors.filter(e => derived.dependencies.contains(e.view))
                if (errorSources.isEmpty) {
                  val updateStart = LocalDateTime.now()
                  Try[UpdateResult] {
                    spark.sparkContext.setJobDescription(s"Updating ${getTaskName(derived)}")
                    derived.refresh
                    Updated(derived, updateStart, LocalDateTime.now())
                  }.recover { case error =>
                    UpdateSelfError(derived, error)
                  }.get
                } else {
                  UpdateSourceError(derived, errorSources)
                }
              } else {
                NoChanges(derived)
              }
            }
          case _: LoggingView[_] => throw new IllegalArgumentException(s"Logging views can't be in dependencies")
        }
        result match {
          case updated: Updated   => go(remains, updated.view +: changed, errors, result +: results)
          case skipped: Skipped   => go(remains, changed, errors, result +: results)
          case error: UpdateError => go(remains, changed, error +: errors, result +: results)
        }
      case _ => results.reverse
    }

    go(linearizeDependencies(derivedViews), changedViews.getOrElse(Vector.empty), Vector.empty, Vector.empty)
  }

  def linearizeDependencies(derivedViews: Seq[WorkflowExecutable[_]]): Vector[WorkflowTask[_]] = {
    val grayed  = new mutable.ArrayBuffer[WorkflowTask[_]]
    val blacked = new mutable.ArrayBuffer[WorkflowTask[_]]

    def go(views: Seq[WorkflowTask[_]]): Unit =
      for (view <- views if !blacked.contains(view))
        if (grayed.contains(view)) {
          throw new Exception(s"Cyclic dependencies detected at $view.")
        } else {
          grayed += view
          view match {
            case action: WorkflowExecutable[_] => go(action.dependencies)
            case _                             => Unit
          }
          blacked += view
        }

    go(derivedViews)

    blacked.toVector
  }

  private[framework] def getTaskName(task: WorkflowTask[_]): String = task match {
    case name: TableName => name.qualifiedName
    case _               => task.getClass.getSimpleName
  }

  def raiseOnError(updateResults: Seq[UpdateResult]): Unit = {
    val errors = updateResults.collect { case e: UpdateSelfError => e.error }
    if (errors.nonEmpty) throw new AggregateException("Update failed", errors)
  }

  /** Get single project value from views
    * Throws exception if views have different projects.
    * Used to replace unsafe, ugly-looking code like views.map(_.project).head in generators
    * @param views Views from which project will be derived
    * @return Project of all views in Seq
    */
  def getViewsProject(views: Seq[WorkflowExecutable[_]]): String =
    views.map(_.project).distinct match {
      case Seq(project) => project
      case Seq()        => throw new IllegalArgumentException("Cannot derive project from empty list of views")
      case seq @ Seq(_, _) =>
        throw new IllegalArgumentException(s"There are more then one options for project: ${seq.mkString(",")}")
    }

  implicit def updateBatchToSeq(b: ExecutionPlan): Seq[Stage] = b.plan

  implicit def seqOfStagesToUpdateBatch(plan: Seq[Stage]): ExecutionPlan = new ExecutionPlan(plan)
}
