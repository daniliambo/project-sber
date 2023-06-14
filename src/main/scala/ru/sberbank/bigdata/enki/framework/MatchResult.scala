package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.types.{DataType, StructField}

sealed trait MatchResult

case object MatchSuccess extends MatchResult

final case class UnmatchedFields(fields: Array[StructField]) extends MatchResult
