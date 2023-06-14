package ru.sberbank.bigdata.enki

import java.sql.Timestamp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import ru.sberbank.bigdata.enki.syntax.timestamp._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.runtime.universe._

package object dataset {

  def extend(dataset: Dataset[_], extendedSchema: StructType): DataFrame = {
    val schema = dataset.schema.fields.map { f =>
      f.name -> f
    }.toMap
    val selector = extendedSchema.fields.map { exField =>
      schema.get(exField.name) match {
        case None =>
          if (exField.nullable) {
            lit(null).cast(exField.dataType).as(exField.name, exField.metadata)
          } else {
            throw new Exception(s"Missing no-nullable field ${exField.name} from source dataset.")
          }
        case Some(field) =>
          if (exField.dataType == field.dataType && exField.nullable || !field.nullable) {
            dataset(exField.name).as(exField.name, exField.metadata)
          } else {
            throw new Exception(s""" Dataset fiels ${exField.name} does not met schema requirements.
                                   | Excepted type ${exField.dataType}, nullable: ${exField.nullable}.
                                   | Actual type ${field.dataType}, nullable: ${field.nullable}.
             """.stripMargin)
          }
      }
    }
    dataset.select(selector: _*)
  }

  def estimateSize(dataset: Dataset[_]): Long = {
    import dataset.sparkSession.implicits._

    val recordCount = dataset.count()
    if (recordCount > 0) {
      val expectedSampleCount = if (recordCount > 1000) 1000 else recordCount
      val sampleData = dataset
        .sample(withReplacement = false, fraction = expectedSampleCount.toDouble / recordCount.toDouble)
        .limit(expectedSampleCount.toInt)
        .persist(StorageLevel.MEMORY_ONLY)
      val sampleCount = sampleData.count()
      val estimateUdf = udf { data: AnyRef =>
        SizeEstimator.estimate(data)
      }
      val estimateAll = dataset.schema.map(field => estimateUdf(dataset(field.name))).reduce(_ + _)
      val sampleSize  = sampleData.select(estimateAll.as("size")).agg(sum($"size")).first().getLong(0)
      val res         = (sampleSize.toDouble * (recordCount.toDouble / sampleCount.toDouble)).toLong
      res
    } else {
      0L
    }
  }

  def timePeriod(start: Timestamp, end: Timestamp, step: Timestamp => Timestamp)(
    implicit spark: SparkSession
  ): Dataset[TimePeriod] = {
    import spark.implicits._

    spark.createDataset {
      @tailrec def go(x: Timestamp, acc: List[Timestamp]): List[Timestamp] =
        if (x.before(end)) go(step(x), x :: acc) else acc

      go(start, Nil).reverse.zipWithIndex.map { case (t, i) =>
        val nxt = step(t)
        TimePeriod(n = i, from = t, to = if (nxt.before(end)) nxt else end)
      }
    }
  }

  def monthRange(start: Timestamp, end: Timestamp)(implicit spark: SparkSession): Dataset[Timestamp] = {
    import spark.implicits._

    timeRange(start.atStartOfMonth, end, _.plusMonth(1))
      .select($"value".as("month"))
      .as[Timestamp]
  }

  def timeRange(start: Timestamp, end: Timestamp, step: Timestamp => Timestamp)(
    implicit spark: SparkSession
  ): Dataset[Timestamp] = {
    import spark.implicits._

    spark.createDataset {
      @tailrec def go(x: Timestamp, acc: List[Timestamp]): List[Timestamp] =
        if (x.before(end)) go(step(x), x :: acc) else acc

      go(start, Nil)
    }.select($"value")
      .as[Timestamp]
  }

  def dateRange(start: Timestamp, end: Timestamp)(implicit spark: SparkSession): Dataset[Timestamp] = {
    import spark.implicits._

    timeRange(start.atStartOfDay, end, _.plusDays(1))
      .select($"value".as("date"))
      .as[Timestamp]
  }

  def toKeyValue(
    dataset: Dataset[_],
    keyColNames: String*
  ): DataFrame = {
    val keyColsSet    = keyColNames.map(_.toLowerCase).toSet
    val valueColNames = dataset.schema.map(_.name).filter(name => !keyColsSet.contains(name.toLowerCase))
    toKeyValue(dataset, keyColNames, "key", valueColNames.map(colName => (colName, "value")).toMap)
  }

  def toKeyValue(
    dataset: Dataset[_],
    primaryKeyNames: Seq[String],
    keyLabel: String,
    valueMappings: Map[String, String]
  ): DataFrame = {
    val sourceCols        = valueMappings.keys.toList
    val targetCols        = valueMappings.values.toList.distinct
    val targetToSourceMap = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
    valueMappings.foreach { case (sourceCol, targetCol) =>
      targetToSourceMap.addBinding(targetCol, sourceCol)
    }
    val sourceSchema = dataset.schema.map(field => (field.name.toLowerCase, field.dataType)).toMap
    val targetTypeMap = targetToSourceMap.map { case (targetColName, sourceColNames) =>
      val firstSourceCol  = sourceColNames.head
      val otherSourceCols = sourceColNames.tail
      val expectedType    = sourceSchema(firstSourceCol.toLowerCase)
      otherSourceCols.foreach { sourceCol =>
        val sourceColType = sourceSchema(sourceCol.toLowerCase)
        if (sourceColType != expectedType)
          throw new Exception(
            s"Column's $sourceCol type does not match. Expecting $expectedType (inferred from $firstSourceCol), actual $sourceColType."
          )
      }
      (targetColName, expectedType)
    }.toMap

    dataset
      .withColumn(
        "__values",
        explode(
          array(
            sourceCols.map(sourceColName =>
              struct(
                lit(sourceColName).as(keyLabel) +:
                  targetCols.map { targetColName =>
                    val targetCol =
                      if (valueMappings(sourceColName) == targetColName)
                        dataset(sourceColName)
                      else
                        lit(null).cast(targetTypeMap(targetColName))
                    targetCol.as(targetColName)
                  }: _*
              )
            ): _*
          )
        )
      )
      .select(
        (primaryKeyNames.map(dataset(_)) :+ col(s"__values.$keyLabel")) ++ targetCols
          .map(targetCol => col(s"__values.$targetCol")): _*
      )
  }

  def zipWithIndex[T: TypeTag](dataset: Dataset[T], indexColName: String = "index", offset: Long = 0): DataFrame = {
    import dataset.sparkSession.implicits._

    def withOffset(rdd: RDD[(T, Long)]) =
      if (offset == 0) rdd else rdd.map { case (row, index) => (row, index + offset) }

    withOffset(dataset.rdd.zipWithIndex).toDF().select($"_2".as(indexColName), $"_1.*")
  }
}
