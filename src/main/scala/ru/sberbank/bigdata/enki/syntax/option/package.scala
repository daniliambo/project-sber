package ru.sberbank.bigdata.enki.syntax

import cats.instances.option._
import cats.syntax.applicative._

package object option {

  implicit def xToOptionX[X](v: X): Option[X] = v.pure[Option]

}
