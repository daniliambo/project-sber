package ru.sberbank.bigdata.enki.test

import org.apache.spark.sql.{Dataset, SparkSession}
import ru.sberbank.bigdata.enki.framework.{SourceView, View, WorkflowTask}

import scala.reflect.runtime.universe._

trait SparkTest {

  implicit protected val spark: SparkSession = LocalSession.localSpark

  protected def asView[T: TypeTag](dataset: Dataset[T]): WorkflowTask[Dataset[T]] = new SourceView[T] {
    override def get(implicit spark: SparkSession): Dataset[T] = dataset

    override def get(replaceMissingColsWithNull: Boolean)(implicit spark: SparkSession): Dataset[T] = dataset
  }
}
