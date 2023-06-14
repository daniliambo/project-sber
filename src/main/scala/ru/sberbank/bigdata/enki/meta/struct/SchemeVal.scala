package ru.sberbank.bigdata.enki.meta.struct

final case class SchemeVal(
  codeScheme: String,
  idInstance: Int,
  idScheme: Int,
  idProduct: Option[Int]
)
