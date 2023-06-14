package ru.sberbank.bigdata.enki.framework

import org.apache.hadoop.fs.Path

final case class TableLocation(qualifiedName: String, path: Option[String])

object TableLocation {
  def apply(qualifiedName: String): TableLocation             = TableLocation(qualifiedName, None)
  def apply(qualifiedName: String, path: Path): TableLocation = TableLocation(qualifiedName, Some(path.toString))
}
