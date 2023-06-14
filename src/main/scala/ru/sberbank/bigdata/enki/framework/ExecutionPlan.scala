package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.SparkSession

class ExecutionPlan(val plan: Seq[Stage]) {

  def exec(implicit spark: SparkSession): Unit =
    plan.foreach(_.action(spark))

  def execStage(stage: String)(implicit spark: SparkSession): Unit =
    plan.find(_.id == stage) match {
      case Some(s) => s.action(spark)
      case None    => throw new Exception(s"Stage $stage not found. Valid stages: ${stageNames.mkString(",")}.")
    }

  def stageNames: Seq[String] = plan.map(_.id)

  //oozie have requirements for stage ids: '([a-zA-Z_]([\-_a-zA-Z0-9])*){1,39}'
  def withOozieficatedStageIds: ExecutionPlan = {
    def maxIdLength = 40

    def replaceInvalidSymbols(s: String) =
      s.replaceAll("^[^a-zA-Z_]|[^\\-_a-zA-Z0-9]", "_")

    //numeration prevents id collision at string truncate
    this.withNumeratedStageIds.map { stage =>
      stage.copy(id = replaceInvalidSymbols("s" + stage.id).take(maxIdLength))
    }
  }

  def withNumeratedStageIds: ExecutionPlan =
    this.zipWithIndex.map { case (stage, index) =>
      stage.copy(
        id = index + "_" + stage.id
      )
    }
}

object ExecutionPlan {

  def apply(target: WorkflowExecutable[_]*): ExecutionPlan =
    new ExecutionPlan(linearizeDependencies(target).flatMap {
      case t: DerivedView[_]  => if (t.persist) Some(refreshStage(t)) else None
      case t: CustomAction[_] => if (t.generateWfStep) Some(customActionStage(t)) else None
      case _                  => None
    })

  private def shortClassName(t: WorkflowTask[_]): String = t.getClass.getName.replaceAll("(^.*[.])|([$]$)", "")

  private def customActionStage(t: CustomAction[_]): Stage =
    Stage(shortClassName(t),
          spark => {
            spark.sparkContext.setJobDescription(s"Running ${getTaskName(t)} ${t.getClass.getName}")
            t.execute(Array())(spark)
          }
    )

  private def refreshStage(t: DerivedView[_]): Stage =
    Stage(shortClassName(t),
          spark => {
            spark.sparkContext.setJobDescription(s"Updating ${getTaskName(t)} ${t.getClass.getName}")
            t.refresh(spark)
          }
    )
}
