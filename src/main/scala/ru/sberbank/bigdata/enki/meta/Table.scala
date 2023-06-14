package ru.sberbank.bigdata.enki.meta

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import ru.sberbank.bigdata.enki.logger.journal.EnkiLogging
import ru.sberbank.bigdata.enki.meta.exception.NoTableException
import ru.sberbank.bigdata.enki.meta.struct.TableVal
import java.net.URLEncoder

object Table {

  def getId(schemeName: String, tableName: String): Either[Throwable, Int] =
    Scheme.getId(schemeName) match {
      case Right(schemeId) =>
        val table = new Table(schemeId, tableName)
        table.getId
      case Left(failure) => Left(failure)
    }
}

class Table(schemeId: Int, tableName: String) extends Entity[TableVal] with EnkiLogging {

  //override val httpSequence: String = s"TbPhTable?filter=%7B%22idScheme%22%3A%20%5B%22${schemeId.toString}%22%5D%7D"
  override val httpSequence: String = Seq(
    "TbPhTable?filter=",
    URLEncoder.encode("""{"idScheme": ["""", DefaultEncoding).replace("+", "%20"),
    schemeId.toString,
    URLEncoder.encode(""""]}""", DefaultEncoding).replace("+", "%20")
  ).mkString

  implicit val decoder: Decoder[TableVal] = deriveDecoder[TableVal]

  def getTableId: Either[Throwable, List[Int]] =
    extractResponseList match {
      case Right(list) =>
        Right(
          for (table <- list.filter(table => (table.codeTable == tableName) && (table.idScheme == schemeId)))
            yield table.idTable
        )
      case Left(msg) => Left(msg)
    }

  def getId: Either[Throwable, Int] =
    getTableId match {
      case Right(ids) =>
        ids.headOption match {
          case Some(head) =>
            if (ids.length > 1) {
              warn("note: table name is not unique")
            }
            Right(head)
          case None => Left(NoTableException(s"unable to find the table: $tableName"))
        }
      case Left(failure) => Left(failure)
    }
}
