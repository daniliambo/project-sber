package ru.sberbank.bigdata.enki.meta

import java.net.URLEncoder

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import ru.sberbank.bigdata.enki.logger.journal.EnkiLogging
import ru.sberbank.bigdata.enki.meta.exception.NoSchemeException
import ru.sberbank.bigdata.enki.meta.struct.SchemeVal

object Scheme {

  def getId(schemeName: String): Either[Throwable, Int] = {
    val scheme = new Scheme(schemeName)
    scheme.getId
  }
}

class Scheme(schemeName: String) extends Entity[SchemeVal] with EnkiLogging {

  //override val httpSequence: String = s"TbPhScheme?filter=%7B%22codeScheme%22%3A%20%5B%22${schemeName.toUpperCase()}%22%5D%7D"
  override val httpSequence: String = Seq(
    "TbPhScheme?filter=",
    URLEncoder.encode("""{"codeScheme": ["""", DefaultEncoding).replace("+", "%20"),
    schemeName.toUpperCase,
    URLEncoder.encode(""""]}""", DefaultEncoding).replace("+", "%20")
  ).mkString

  implicit val decoder: Decoder[SchemeVal] = deriveDecoder[SchemeVal]

  def getSchemeId: Either[Throwable, List[Int]] =
    extractResponseList match {
      case Right(list) =>
        Right(
          for (scheme <- list.filter(_.codeScheme == schemeName.toUpperCase()))
            yield scheme.idScheme
        )
      case Left(failure) => Left(failure)
    }

  def getId: Either[Throwable, Int] =
    getSchemeId match {
      case Right(ids) =>
        ids.headOption match {
          case Some(head) =>
            if (ids.length > 1) warn("notice: scheme name is not unique")
            Right(head)
          case None => Left(NoSchemeException(s"unable to find the scheme: $schemeName"))
        }
      case Left(failure) => Left(failure)
    }
}
