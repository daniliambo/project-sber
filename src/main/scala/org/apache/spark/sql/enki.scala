package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical._

object enki {

  def getLogicalPlan(dataset: Dataset[_]): LogicalPlan = dataset.logicalPlan

  def createDataFrame(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame =
    Dataset.ofRows(sparkSession, logicalPlan)

  def runCommand(session: SparkSession, name: String)(command: LogicalPlan): Unit = {
    val qe = session.sessionState.executePlan(command)
    try {
      val start = System.nanoTime()
      // call `QueryExecution.toRDD` to trigger the execution of commands.
      qe.toRdd
      val end = System.nanoTime()
      session.listenerManager.onSuccess(name, qe, end - start)
    } catch {
      case e: Exception =>
        session.listenerManager.onFailure(name, qe, e)
        throw e
    }
  }
}
