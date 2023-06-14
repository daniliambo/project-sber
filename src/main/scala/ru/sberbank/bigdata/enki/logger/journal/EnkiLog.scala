package ru.sberbank.bigdata.enki.logger.journal

import io.circe.generic.auto._
import io.circe.syntax._

final case class EnkiLog(
  source: String,
  `class`: String,
  message: String
) {

  def toMultiLineJson: String = this.asJson.spaces2

  def toSingleLineJson: String = this.asJson.noSpaces

}
