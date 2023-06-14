package ru.sberbank.bigdata.enki.framework.writer

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.InsertIntoTable
import org.apache.spark.sql.enki.{getLogicalPlan, runCommand}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset}

import scala.reflect.runtime.universe._

package object partitions {

  /** Строит индекс, указывающий какой ключ лежит в каком разделе.
    */
  def partitionIndex[K: TypeTag](
    dataset: Dataset[_],
    keyColumns: Seq[String],
    partitionColumns: Seq[String]
  ): Dataset[(K, Map[String, Option[String]])] = {
    val spark = dataset.sparkSession
    import spark.implicits._

    dataset
      .select(
        if (keyColumns.length == 1) {
          col(keyColumns.head).as("key")
        } else {
          struct(keyColumns.map(col): _*).as("key")
        },
        map(partitionColumns.flatMap(colName => Seq(lit(colName), col(colName))): _*).as("partition")
      )
      .as[(K, Map[String, Option[String]])]
  }

  def updatePartition(
    tableName: String,
    partition: Map[String, Option[String]],
    dataset: Dataset[_],
    keys: Seq[String],
    deleted: Column,
    tmpTablePrefix: String = "__tmp"
  ): Unit = {
    val spark = dataset.sparkSession

    val insertedOrUpdated = dataset.where(not(deleted))
    val currentPart = spark
      .table(tableName)
      .where(
        partition.map { case (colName: String, b: Option[String]) =>
          b.map(value => col(colName) === lit(value)).getOrElse(col(colName).isNull)
        }.reduce(_.and(_))
      )
    val unchanged    = currentPart.join(dataset, keys, "leftanti")
    val updateResult = unchanged.union(insertedOrUpdated.select(unchanged.schema.map(f => col(f.name)): _*))
    val tableIdent   = dataset.sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
    val tmpTableName = tableIdent.database match {
      case Some(db) => s"$db.${tmpTablePrefix}_${tableIdent.table}"
      case None     => s"${tmpTablePrefix}_${tableIdent.table}"
    }
    updateResult.write.saveAsTable(tmpTableName)
    try {
      val persisted = spark.table(tmpTableName)
      overwritePartition(tableName, partition, persisted)
    } finally spark.sql(s"drop table if exists $tmpTableName")
  }

  def overwritePartition(tableName: String, partition: Map[String, Option[String]], dataset: Dataset[_]): Unit = {
    val tableIdent = dataset.sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)

    runCommand(dataset.sparkSession, "insertInto") {
      InsertIntoTable(
        table                = UnresolvedRelation(tableIdent),
        partition            = partition,
        query                = getLogicalPlan(dataset.drop(partition.keys.toSeq: _*)),
        overwrite            = true,
        ifPartitionNotExists = false
      )
    }
  }

}
