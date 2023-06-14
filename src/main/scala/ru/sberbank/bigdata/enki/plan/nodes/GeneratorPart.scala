package ru.sberbank.bigdata.enki.plan.nodes

import org.apache.spark.sql.catalyst.expressions.Generator
import ru.sberbank.bigdata.enki.plan.columns.ColumnReference

final case class GeneratorPart(
  generator: Generator,
  join: Boolean,
  outer: Boolean,
  qualifier: String,
  columns: Vector[ColumnReference]
)
