package ru.sberbank.bigdata.enki.framework

trait CustomAction[T] extends WorkflowTask[T] with Executable[T] with Name {
  def generateWfStep: Boolean = true

  def description: Option[String] = None
}
