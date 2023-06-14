package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions._

sealed trait RowStatus

case object RowUnknown extends RowStatus

case object RowDeleted extends RowStatus

/** Трейт для таблиц, содержащих исторические данные. Возможно использование как с таблицами-источниками, так и со
  * вторичными представлениями.
  *
  * @tparam T Тип записи.
  */
trait HistoricalView[T] {
  self: WorkflowTask[Dataset[T]] with HasEncoder[T] =>

  def id(dataFrame: Dataset[T]): Seq[Column]

  def version(dataFrame: Dataset[T]): Seq[Column]

  def notDeleted(dataFrame: Dataset[T]): Option[Column]

  def actual(implicit spark: SparkSession): Dataset[T] = {
    val src = recent
    notDeleted(src) match {
      case Some(f) => src.filter(f)
      case None    => src
    }
  }

  def recent(implicit spark: SparkSession): Dataset[T] = {
    import spark.implicits._

    def filterByRowNumber(src: Dataset[T]): Dataset[T] =
      src
        .filter(id(src).map(_.isNotNull).reduce(_ || _))
        .withColumn("__rn", row_number().over(partitionBy(id(src): _*).orderBy(version(src).map(_.desc): _*)))
        .filter($"__rn" === 1)
        .drop("__rn")
        .as[T](encoder(src.schema))

    val res: Dataset[T] = this match {
      case d: DerivedTable[T] => filterByRowNumber(d.get)
      case s: SourceView[T]   => filterByRowNumber(s.get)
      case _ =>
        throw new UnsupportedOperationException(
          s"Getting dataset from the instance of ${this.getClass.getName} is not supported."
        )
    }
    res
  }
}

trait OdsHistoricalView[T] extends HistoricalView[T] {
  self: WorkflowTask[Dataset[T]] with HasEncoder[T] =>

  override def version(dataFrame: Dataset[T]): Seq[Column] = Seq(col("ods_validfrom"))

  override def notDeleted(dataFrame: Dataset[T]): Option[Column] = Some(col("ods_opc") =!= lit("D"))
}
