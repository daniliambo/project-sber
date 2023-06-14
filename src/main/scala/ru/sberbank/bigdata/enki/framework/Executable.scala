package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.SparkSession

trait Executable[T] {
  this: WorkflowTask[T] =>
  def execute(args: Array[String])(implicit spark: SparkSession): T

  def dependencies: Seq[WorkflowTask[_]]

  def project: String

  def generateWfStep: Boolean
}
