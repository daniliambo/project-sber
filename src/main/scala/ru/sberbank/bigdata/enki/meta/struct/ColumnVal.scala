package ru.sberbank.bigdata.enki.meta.struct

final case class ColumnVal(
  codeColumn: String,
  idColumn: Int,
  idTable: Int,
  dataPrecision: Option[Int] = None,
  dataScale: Option[Int]     = None,
  idDatatype: Int,
  isPkdUk: Boolean,
  nameColumn: String
)
