package ru.sberbank.bigdata.enki.sql

sealed trait MatchResult

case object MatchSuccess extends MatchResult

final case class MatchError(errorMessage: String) extends MatchResult
