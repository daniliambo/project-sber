package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.{Dataset, SparkSession}

trait View[T] extends WorkflowTask[Dataset[T]] {
  def get(implicit spark: SparkSession): Dataset[T]
}

trait SourceView[T] extends View[T] {
  def generateWfStep: Boolean = true

  def get(replaceMissingColsWithNull: Boolean = false)(implicit spark: SparkSession): Dataset[T]
}

trait LoggingView[T] extends View[T] {
  def generateWfStep: Boolean = true
}

trait DerivedView[T] extends View[T] with Executable[Dataset[T]] {
  def generateWfStep: Boolean = persist

  def persist: Boolean = true

  def appendable: AppendConfiguration = AppendConfiguration(isAppendable = false, AppendExistingAction.DummyMode)

  def refresh(implicit spark: SparkSession): Boolean

  def delete(implicit spark: SparkSession): Unit
}
