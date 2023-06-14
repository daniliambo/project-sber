package ru.sberbank.bigdata.enki.syntax.collection

import ru.sberbank.bigdata.enki.syntax.option._

package object seq {

  implicit final class SeqSyntax[A](seq: Seq[A]) {
    def noneIfEmpty: Option[Seq[A]]                  = if (seq.isEmpty) None else seq
    def mapTo[B](implicit converter: A => B): Seq[B] = seq.map(converter)
  }

}
