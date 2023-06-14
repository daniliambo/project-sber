package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import ru.sberbank.bigdata.enki.schema._
import ru.sberbank.bigdata.enki.transformation._

trait SourceTableConfigurator[T] extends Encoded[T] with TableName with HasTag[T] {

  def schemasPrefix: Option[String] = None

  /** Can be added as suffix after schema with underscore. */
  def schemasSuffix: Option[String] = None

  abstract override def decode(dataFrame: DataFrame)(implicit session: SparkSession): Dataset[T] = {
    val renamed = mapColumns(dataFrame, configuration.mappedColumns)
    val converted = if (configuration.enableTypeConversion) {
      val dstSchema = bareSchemaOf[T](tag, session)
      convert(renamed, dstSchema)
    } else {
      renamed
    }
    super.decode(converted)
  }

  /** @param mappedColumns column name -> column expression.
    */
  private def mapColumns(data: DataFrame, mappedColumns: Map[String, String]): DataFrame = {
    val lowered          = mappedColumns.map { case (key: String, value: String) => key.toLowerCase -> value.toLowerCase }
    val unaffectedFields = data.schema.filter(f => !lowered.contains(f.name.toLowerCase))
    data.select(unaffectedFields.map(f => data(f.name)) ++ lowered.map { case (to: String, from: String) =>
      expr(from).as(to)
    }: _*)
  }

  def configuration: SourceTableConfiguration = SourceTableConfiguration()

  override def qualifiedName: String = ""

}
