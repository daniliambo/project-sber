package ru.sberbank.bigdata.enki.keys

import scala.annotation.StaticAnnotation

trait KeyAnnotation extends StaticAnnotation {
  def order: Int
}
