package ru.sberbank.bigdata.enki.meta

import scala.sys.process._
import io.circe.{Decoder, Json}
import io.circe.parser.parse

abstract class Entity[T] {
  val httpSequence: String

  def request: Seq[String] =
    Seq("curl",
        "--negotiate",
        "-u : ",
        "-X",
        "GET",
        s"http://tkli-ahd0013.dev.df.sbrf.ru:48094/$httpSequence",
        "-H",
        "\"accept: application/json\""
    )

  def getRawJson: String = request.!!

  def getJson: Json = parse(getRawJson) match {
    case Right(value) => value
    case Left(_)      => Json.Null
  }

  def extractResponseList(implicit decoder: Decoder[T]): Either[Throwable, List[T]] = {
    val cursor = getJson.hcursor

    cursor.downField("response").as[List[T]]
  }
}
