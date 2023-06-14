package ru.sberbank.bigdata.enki.meta.struct

final case class TableVal(
  codeTable: String,
  idScheme: Int,
  idSchemePath: Option[String],
  idTable: Int,
  descriptionTable: Option[String],
  inputFormat: Option[String],
  isActual: Option[Boolean],
  isSupplementalLogging: Option[Boolean],
  outputFormat: Option[String],
  owner: Option[String],
  sdsLocation: Option[String],
  sysvalidate: Option[String],
  typeTable: String,
  viewExpText: Option[String],
  viewOrgText: Option[String]
)
