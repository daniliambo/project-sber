package ru.sberbank.bigdata.enki.framework.writer

trait Sorted {

  def sortColumns: Option[Seq[String]] = None

}
