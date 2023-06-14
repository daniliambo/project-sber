package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.SparkSession

//TODO: возможно следует добавить description сюда.
final case class UpdateStage(id: String, action: SparkSession => Unit)

object UpdateStage {

  def apply(id: String, action: SparkSession => Unit, description: String): UpdateStage =
    UpdateStage(id, withJobDescription(action, description))

  def apply(t: WorkflowExecutable[_]): UpdateStage = t match {
    case v: DerivedView[_] =>
      UpdateStage(getTaskName(v), withJobDescription(v.refresh(_), s"Updating ${getTaskName(v)}"))
    case c: CustomAction[_] =>
      UpdateStage(getTaskName(c), withJobDescription(c.execute(Array())(_), s"Running ${getTaskName(c)}"))
  }

  private def withJobDescription(action: SparkSession => Unit, description: String): SparkSession => Unit = spark => {
    spark.sparkContext.setJobDescription(description)
    try action(spark)
    finally spark.sparkContext.setJobDescription(null)
  }
}
