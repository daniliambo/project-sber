package ru.sberbank.bigdata.enki.dataset

import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, LeftSemi}
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.{Column, DataFrame, Dataset}

object PartialBroadcastJoin {
  private val defaultSampleFraction = 0.0001
  private val defaultMaxKeyCount    = 1000

  def topKeys(
    dataset: Dataset[_],
    keyColumns: Seq[Column],
    maxKeyCount: Int       = defaultMaxKeyCount,
    sampleFraction: Double = defaultSampleFraction
  ): DataFrame =
    dataset
      .sample(withReplacement = false, sampleFraction)
      .groupBy(keyColumns.zipWithIndex.map(c => c._1.as("__key" + c._2.toString)): _*)
      .count()
      .orderBy(col("count").desc)
      .limit(maxKeyCount)
      .drop("count")

  /** Join two datasets using broadcast join for records whith provided set of keys and shuffle join for remaining records.
    *
    * @param left            Left dataset. Usually large dataset with extreme data skew on specified keys.
    * @param right           Right dataset. Usually smaller dataset, containing data to be broadcasted.
    * @param joinConditions  Restricted join conditions.
    * @param keysToBroadcast Keys to broadcast. Column order should match column order in join conditions.
    *                        This dataset will be scanned a number of times, make sure to catch it.
    * @param joinType        Type of the join.
    * @return Joined datasets.
    */
  def partialBroadcastJoin(
    left: Dataset[_],
    right: Dataset[_],
    joinConditions: Seq[JoinCondition],
    keysToBroadcast: Dataset[_],
    joinType: String
  ): DataFrame = {
    JoinType(joinType) match {
      case supported if List(LeftOuter, LeftSemi, Inner).contains(supported) => ()
      case unsupported                                                       => throw new IllegalArgumentException(s"Unsupported join type: $unsupported")
    }

    val keysToNames = keysToBroadcast.schema.map(_.name)

    val leftToTop = joinConditions
      .zip(keysToNames)
      .map(c => c._1.applyTo(c._1.leftCol, keysToBroadcast(c._2), ensureNullSafe = true))
      .reduce(_ && _)

    val leftTop   = left.join(broadcast(keysToBroadcast), leftToTop, "leftsemi")
    val leftOther = left.join(broadcast(keysToBroadcast), leftToTop, "leftanti")

    val rightToTop = joinConditions
      .zip(keysToNames)
      .map(c => c._1.applyTo(c._1.rightCol, keysToBroadcast(c._2), ensureNullSafe = true))
      .reduce(_ && _)

    val rightTop   = right.join(broadcast(keysToBroadcast), rightToTop, "leftsemi")
    val rightOther = right.join(broadcast(keysToBroadcast), rightToTop, "leftanti")

    val leftToRight = joinConditions.map(c => c.toExpr).reduce(_ && _)

    val leftTopJoined   = leftTop.join(broadcast(rightTop), leftToRight, joinType)
    val leftOtherJoined = leftOther.join(rightOther, leftToRight, joinType)

    leftTopJoined.union(leftOtherJoined)
  }

  sealed trait JoinCondition {
    def leftCol: Column

    def rightCol: Column

    def applyTo(l: Column, r: Column, ensureNullSafe: Boolean = false): Column

    def toExpr: Column = applyTo(leftCol, rightCol)
  }

  final case class Equal(leftCol: Column, rightCol: Column) extends JoinCondition {

    override def applyTo(l: Column, r: Column, ensureNullSafe: Boolean): Column =
      if (ensureNullSafe) {
        l <=> r
      } else {
        l === r
      }
  }

  final case class EqualNullSafe(leftCol: Column, rightCol: Column) extends JoinCondition {
    override def applyTo(l: Column, r: Column, ensureNullSafe: Boolean): Column = l <=> r
  }

  implicit class SkewedjoinDatasetExtensions(dataset: Dataset[_]) {

    /** Join two datasets partially broadcasting row from the right dataset.
      *
      * @param right          Right dataset.
      * @param joinType       Type of the join.
      * @param joinConditions Restricted join conditions.
      * @return Joined datasets.
      *
      *         This function will imply additional dataset scan, which can lead to cascading underlying tables scans.
      *         Use version with keysToBroadcast parameter when this undesired.
      */
    def partialBroadcastJoin(right: Dataset[_], joinType: String, joinConditions: JoinCondition*): DataFrame = {
      // ключи не кешируются, т.к. это приводит к игнорированию broadcast хинтов при фильтрации по ключам.
      val keysToBroadcast = dataset.topKeys(joinConditions.map(_.leftCol): _*)
      PartialBroadcastJoin.partialBroadcastJoin(dataset, right, joinConditions, keysToBroadcast, joinType)
    }

    def topKeys(columns: Column*): DataFrame =
      PartialBroadcastJoin.topKeys(dataset, columns)

    def partialBroadcastJoin(
      right: Dataset[_],
      keysToBroadcast: Dataset[_],
      joinType: String,
      joinConditions: JoinCondition*
    ): DataFrame =
      PartialBroadcastJoin.partialBroadcastJoin(dataset, right, joinConditions, keysToBroadcast, joinType)
  }
}
