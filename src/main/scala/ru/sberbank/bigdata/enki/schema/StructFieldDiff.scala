package ru.sberbank.bigdata.enki.schema

import org.apache.spark.sql.types.StructField

sealed trait StructFieldDiff {
  def pretty: String
}

final case class Matched(field: StructField) extends StructFieldDiff {
  override def pretty: String = field.name
}

final case class LeftOnly(lfield: StructField) extends StructFieldDiff {
  override def pretty: String = lfield.name
}

final case class RightOnly(rfield: StructField) extends StructFieldDiff {
  override def pretty: String = rfield.name
}

final case class Differ(lfield: StructField, rfield: StructField) extends StructFieldDiff {
  override def pretty: String = s"${lfield.name} in types ${lfield.dataType} / ${rfield.dataType}"
}

final case class StructTypeDiff(fields: Array[StructFieldDiff]) extends Seq[StructFieldDiff] {
  override def apply(idx: Int): StructFieldDiff = fields(idx)

  override def length: Int = fields.length

  override def iterator: Iterator[StructFieldDiff] = fields.iterator

  def onlyMatched: Boolean =
    fields.collectFirst { case notMatched @ (_: LeftOnly | _: RightOnly | _: Differ) =>
      notMatched
    }.isEmpty

  def differences: StructTypeDiff =
    StructTypeDiff(fields.filterNot {
      case _: Matched => true
      case _          => false
    })

  def prettyPrint: String = {
    val (matches, lefts, rights, diffs) = fields.foldLeft(
      (Vector.empty[Matched], Vector.empty[LeftOnly], Vector.empty[RightOnly], Vector.empty[Differ])
    ) { case ((matched, left, right, diff), r) =>
      r match {
        case m: Matched   => (m +: matched, left, right, diff)
        case l: LeftOnly  => (matched, l +: left, right, diff)
        case r: RightOnly => (matched, left, r +: right, diff)
        case d: Differ    => (matched, left, right, d +: diff)
      }
    }

    def genRow(name: String, values: Vector[StructFieldDiff]): String =
      if (values.isEmpty) "" else s"$name: ${values.map(_.pretty).mkString(",")}"

    Vector(genRow("Matched", matches),
           genRow("Only in Left", lefts),
           genRow("Only in Right", rights),
           genRow("Different", diffs)
    ).mkString("\n")

  }

}
