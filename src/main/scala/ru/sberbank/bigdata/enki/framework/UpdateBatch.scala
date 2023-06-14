package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.SparkSession

class UpdateBatch(val plan: Seq[UpdateStage]) {

  def exec(implicit spark: SparkSession): Unit =
    plan.foreach(_.action(spark))

  def exec(stage: String)(implicit spark: SparkSession): Unit =
    plan.find(_.id == stage) match {
      case Some(s) => s.action(spark)
      case None    => throw new Exception(s"Stage $stage not found. Valid stages: ${stageNames.mkString(",")}.")
    }

  def stageNames: Seq[String] = plan.map(_.id)
}

object UpdateBatch {

  def apply(target: WorkflowExecutable[_]*): UpdateBatch =
    new UpdateBatch(linearizeDependencies(target).flatMap {
      case t: DerivedView[_]  => if (t.persist) Some(UpdateStage(t)) else None
      case t: CustomAction[_] => if (t.generateWfStep) Some(UpdateStage(t)) else None
      case _                  => None
    })
}
