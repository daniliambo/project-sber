package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.SparkSession
import ru.sberbank.bigdata.enki.schema._

trait SchemaDiffTool[T] {
  self: TableName with HasTag[T] =>

  def schemaDiff(implicit session: SparkSession): StructTypeDiff = {
    val left = bareSchemaOf[T](tag, session)
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var right = session.read.table(qualifiedName).schema
    diff(left, right).differences
  }
}
