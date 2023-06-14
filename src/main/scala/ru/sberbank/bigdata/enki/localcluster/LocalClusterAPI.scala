package ru.sberbank.bigdata.enki.localcluster

import cats.syntax.option._
import org.apache.spark.sql.{DataFrameWriter, Dataset, SaveMode, SparkSession}
import ru.sberbank.bigdata.enki.framework._
import ru.sberbank.bigdata.enki.logger.journal.EnkiLogging

import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe.{typeOf, MethodSymbol}

object LocalClusterAPI extends EnkiLogging {

  def refreshAndPublish(table: DerivedTable[_])(implicit spark: SparkSession): Unit = ()

  def refreshTable(table: DerivedTable[_])(implicit spark: SparkSession): Unit = {
    val tableName = table.qualifiedName
    info(s"Refreshing table '$tableName'")

    if (localCalculationIsImpossible(table)) {
      warn("Local Calculation is impossible, saving result of getEmpty method")
      val stgTableLocation = TableLocation(table.qualifiedName, table.path)
      manuallyWriteTable(table.getEmpty(), stgTableLocation, table.partitionColNames)
    } else {
      table.refresh
    }

    info(s"Refreshed table '$tableName'")
  }

  def saveEmptySourceTable(table: Table[_])(implicit spark: SparkSession): Unit = {
    val tableName = table.qualifiedName
    info(s"Saving source table '$tableName'")

    val (schema, qualifiedName) = table match {
      case otherTable               => (otherTable.schema, otherTable.qualifiedName)
    }

    val sourceTableLocation =
      TableLocation(qualifiedName, table.path.getOrElse(s"").some)

    manuallyWriteTable(table.getEmpty(), sourceTableLocation, table.partitionColNames)

    info(s"Saved source table '$tableName'")
  }

  def saveTableToPublishLocation(table: Any)(implicit spark: SparkSession): Unit = ()

  def createSchema(schema: String)(implicit spark: SparkSession): Unit = {
    info(s"Creating schema '$schema' if not exists")
    if (schema.isEmpty) {
      // No exception is thrown there, because not persisted views from sberbank data sources have empty schema
      warn("Your dependencies contains views with no schema defined")
    } else {
      spark.sql(s"create database if not exists $schema")
    }
  }

  def getProjectTable(finalTables: Seq[WorkflowExecutable[_]]): Vector[DerivedTable[_]] = {
    val projects = finalTables.map(_.project).toSet
    val derivedDependencies = linearizeDependencies(finalTables).collect { case table: DerivedTable[_] => table }
      .filter(_.persist)

    derivedDependencies.filter(table => projects.contains(table.project))
  }

  private def manuallyWriteTable(dataset: Dataset[_], location: TableLocation, partColNames: Seq[String])(
    implicit spark: SparkSession
  ): Unit = {

    implicit class writerExt(writer: DataFrameWriter[_]) {
      def withOptPartitioning: DataFrameWriter[_] =
        if (partColNames.nonEmpty)
          writer.partitionBy(partColNames: _*)
        else
          writer
    }

    dataset.write.withOptPartitioning
      .mode(SaveMode.Overwrite)
      /* Parquet cause error for tables build over org_okved_rosstat and ul_organization_rosstat
       * from custom_cb_akm_integrum. */
      .format("parquet")
      .option("path", location.path.get)
      .saveAsTable(location.qualifiedName)

  }

  /** Checks whether any method in table has a [[localCalculationImpossible]] annotation */
  private[enki] def localCalculationIsImpossible(table: DerivedTable[_]): Boolean =
    currentMirror
      .classSymbol(table.getClass)
      .baseClasses
      .map(_.info.decls)
      .flatMap {
        _.collect { case ms: MethodSymbol => ms }
          .flatMap(_.annotations)
          .collect {
            case annotation if annotation.tree.tpe <:< typeOf[localCalculationImpossible] => annotation
          }
      }
      .nonEmpty

}
