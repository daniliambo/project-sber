package ru.sberbank.bigdata.enki.syntax.collection

package object map {

  implicit class MapExtensions[K, V](map: Map[K, V]) {
    def +?(kvop: (K, Option[V])): Map[K, V] = kvop._2.map(value => map + (kvop._1 -> value)).getOrElse(map)
  }

}
