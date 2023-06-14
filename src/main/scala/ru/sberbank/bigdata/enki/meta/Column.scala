package ru.sberbank.bigdata.enki.meta

import java.net.URLEncoder

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import ru.sberbank.bigdata.enki.logger.journal.EnkiLogging
import ru.sberbank.bigdata.enki.meta.struct.ColumnVal

object Column extends EnkiLogging {

  def getDescr(schemeName: String, tableName: String): Map[String, String] =
    Table.getId(schemeName, tableName) match {
      case Right(tableId) =>
        val column = new Column(tableId)
        column.getColumnDescr match {
          case Right(descr)  => descr
          case Left(failure) => error(failure.getMessage); Map()
        }
      case Left(failure) => error(failure.getMessage); Map()
    }

}

class Column(tableId: Int) extends Entity[ColumnVal] {

  //override val httpSequence: String = s"TbPhColumn?filter=%7B%22idTable%22%3A%20%5B%22${tableId.toString}%22%5D%7D"
  override val httpSequence: String = Seq(
    "TbPhColumn?filter=",
    URLEncoder.encode("""{"idTable": ["""", DefaultEncoding).replace("+", "%20"),
    tableId.toString,
    URLEncoder.encode(""""]}""", DefaultEncoding).replace("+", "%20")
  ).mkString

  implicit val decoder: Decoder[ColumnVal] = deriveDecoder[ColumnVal]

  def getColumnDescr: Either[Throwable, Map[String, String]] =
    extractResponseList match {
      case Right(list) =>
        val lmap = {
          for (column <- list) yield column.codeColumn -> column.nameColumn
        }
        Right(Map(lmap: _*))
      case Left(failure) => Left(failure)
    }
}
