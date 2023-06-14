package ru.sberbank.bigdata.enki.localcluster.localexec

import cats.effect.Sync
import cats.syntax.all._
import org.apache.spark.sql.{Row, SparkSession}
import ru.sberbank.bigdata.enki.framework.{DerivedTable, SourceTable, Table, WorkflowTask}
import ru.sberbank.bigdata.enki.localcluster.LocalClusterAPI._
import ru.sberbank.bigdata.enki.localcluster.withClassLoader0
import ru.sberbank.bigdata.enki.logger.Log4Enki

import scala.collection.mutable.{HashSet => MutHashSet}
import scala.reflect.runtime.universe.typeOf

abstract class LocalExecution[F[_]] {

  protected val logger: Log4Enki[F]

  def run(finalTables: Seq[DerivedTable[_]], classLoader: Option[ClassLoader] = None)(
    implicit spark: SparkSession
  ): F[Unit]

}

object LocalExecution {

  def create[F[_]: Sync: Log4Enki]: LocalExecution[F] = new LocalExecutionImpl[F]

  private class LocalExecutionImpl[F[_]](implicit val logger: Log4Enki[F], F: Sync[F]) extends LocalExecution[F] {

    private def needToRefresh(table: DerivedTable[_], projects: Set[String]): Boolean =
      if (!table.persist) {
        // Skip not persisted tables
        false
      } else if (projects.contains(table.project)) {
        // Always refresh project tables
        true
      } else if (table.getTypeParameter == typeOf[Row] && table.structMap.isEmpty) {
        // Refresh outer tables if their schemas cannot be inferred
        // from type parameter or structMap
        true
      } else {
        false
      }

    // Change linearizeDependencies function from framework package in order to
    // reduce amount of calculated tables
    private def dependencies(finalTables: Seq[DerivedTable[_]])(implicit spark: SparkSession): Vector[Table[_]] = {
      val projects = finalTables.map(_.project).toSet

      val blacked = MutHashSet.empty[WorkflowTask[_]]
      val grayed  = MutHashSet.empty[WorkflowTask[_]]
      val tables  = Vector.newBuilder[Table[_]]

      def go(tasks: Seq[WorkflowTask[_]]): Unit =
        for (task <- tasks if !blacked.contains(task)) {
          if (grayed.contains(task)) throw new Exception(s"Cyclic dependency detected at $task")

          grayed += task

          task match {
            case sourceTable: SourceTable[_] => tables += sourceTable

            case derivedTable: DerivedTable[_] =>
              if (needToRefresh(derivedTable, projects)) go(derivedTable.dependencies)
              tables += derivedTable

            case _ => ()
          }

          blacked += task
        }

      go(finalTables)

      tables.result()
    }

    override def run(finalTables: Seq[DerivedTable[_]], classLoader: Option[ClassLoader] = None)(
      implicit spark: SparkSession
    ): F[Unit] = {
      val projects = finalTables.map(_.project).toSet

      val tables = dependencies(finalTables)

      val schemas = tables.map(_.schema).distinct

      val publishSchemas = Seq.empty

      val tablesToRefresh = tables.collect {
        case derivedTable: DerivedTable[_] if needToRefresh(derivedTable, projects) => derivedTable
      }

      val tablesToSave = tables.filter {
        case sourceTable: SourceTable[_] =>
          if (sourceTable.structMap.isDefined) {
            true // Can infer a schema from manually defined struct
          } else {
            val isRowStruct = sourceTable.getTypeParameter == typeOf[Row]

            if (isRowStruct)
              logger.error(
                s"Source table ${sourceTable.qualifiedName} has Row type and no structMap provided, " +
                  s"cannot infer schema!"
              )

            !isRowStruct
          }

        case derivedTable: DerivedTable[_] if !needToRefresh(derivedTable, projects) =>
          true

        case _ => false
      }

      def withClassLoaderF[U](action: => U): F[U] = F.delay(withClassLoader0(action)(classLoader))

      for {
        _ <- withClassLoaderF(schemas.foreach(createSchema))
        _ <- withClassLoaderF(publishSchemas.foreach(createSchema))
        _ <- withClassLoaderF(tablesToSave.foreach(saveEmptySourceTable))
        _ <- withClassLoaderF(tablesToRefresh.foreach(refreshAndPublish))
      } yield ()
    }

  }

}
