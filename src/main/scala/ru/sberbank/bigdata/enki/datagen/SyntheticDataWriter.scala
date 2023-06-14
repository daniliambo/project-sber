package ru.sberbank.bigdata.enki.datagen

import cats.effect.IO
import org.apache.spark.sql.SparkSession
import ru.sberbank.bigdata.enki.ctl.dependencies.sourceDependencies
import ru.sberbank.bigdata.enki.datagen.SyntheticDataGenerator.sampleDataset
import ru.sberbank.bigdata.enki.framework.{SourceTable, TableName, WorkflowExecutable, WorkflowTask}

object SyntheticDataWriter {

  def writeSourceTables(finalViews: Seq[WorkflowExecutable[_]], rowCount: Int = 20)(
    implicit spark: SparkSession
  ): IO[Unit] = {
    info(s"Source dependencies: ${sourceDependencies(finalViews).mkString(",")}")
    IO {
      sourceDependencies(finalViews).foreach {
        case vw: SourceTable[_] =>
          SyntheticDataWriter.writeSingleTable(vw)
        case vw =>
          throw new IllegalArgumentException(
            s"Dependency '$vw' is not a source table! Writing is not supported."
          )
      }
    }
  }

  def writeSingleTable(targetView: WorkflowTask[_] with TableName, rowCount: Int = 20)(
    implicit spark: SparkSession
  ): IO[Unit] = {
    val database = targetView.schema
    val table    = targetView.name
    val path     = tableLocation(database, table)

    IO {
      ensureDatabase(database)
      info(s"View: ${targetView.qualifiedName}. Saving data to $path ...")
      sampleDataset(targetView, rowCount).write
        .option("location", path)
        .format("parquet")
        .saveAsTable(targetView.qualifiedName)
    }
  }
}
