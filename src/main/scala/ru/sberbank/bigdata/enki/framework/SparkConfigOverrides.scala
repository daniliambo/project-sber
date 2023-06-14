package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.internal.SQLConf
import ru.sberbank.bigdata.enki.syntax.collection.map._

/** Override default SparkSession parameters
  *
  * @param shufflePartitions            The default number of partitions to use when shuffling data for joins or aggregations.
  * @param constraintPropagationEnabled When true, the query optimizer will infer and propagate data constraints in the query
  */
@SuppressWarnings(
  Array("org.wartremover.warts.FinalCaseClass")
) // Class is overridden in tests. And Encoder is automatically derived by circe for case classes.
case class SparkConfigOverrides(
  shufflePartitions: Option[Int],
  constraintPropagationEnabled: Option[Boolean]
) {

  def toMap: Map[String, String] =
    Map.empty[String, String] +?
      (SQLConf.SHUFFLE_PARTITIONS.key             -> shufflePartitions.map(_.toString)) +?
      (SQLConf.CONSTRAINT_PROPAGATION_ENABLED.key -> constraintPropagationEnabled.map(_.toString))
}

object SparkConfigOverrides {

  def empty = SparkConfigOverrides(
    shufflePartitions            = None,
    constraintPropagationEnabled = None
  )
}
