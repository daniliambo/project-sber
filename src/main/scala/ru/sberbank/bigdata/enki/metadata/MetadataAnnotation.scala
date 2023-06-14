package ru.sberbank.bigdata.enki.metadata

import scala.annotation.StaticAnnotation

trait MetadataAnnotation extends StaticAnnotation {
  def key: String

  def value: String
}
