package ru.sberbank.bigdata.enki.syntax

package object tuple {

  implicit class tuple2Syntax[A](t: (A, A)) {
    def map[R](f: A => R, g: A => R): (R, R) = (f(t._1), g(t._2))
    def map[R](f: A => R): (R, R)            = (f(t._1), f(t._2))
    def right                                = t._2
  }

  implicit class tuple3Syntax[A](t: (A, A, A)) {
    def map[R](f: A => R): (R, R, R) = (f(t._1), f(t._2), f(t._3))
    def map[R](f: (A, A, A) => R): R = f(t._1, t._2, t._3)
    def reduce(op: (A, A) => A): A   = op(op(t._1, t._2), t._3)
  }

  implicit class tuple4Syntax[A](t: (A, A, A, A)) {
    def map[R](f: A => R): (R, R, R, R) = (f(t._1), f(t._2), f(t._3), f(t._4))
    def map[R](f: (A, A, A, A) => R): R = f(t._1, t._2, t._3, t._4)
  }

  implicit class Tuple2AnyOptExtensions[A, B](tuple: (A, Option[B])) {
    def toMap: Map[A, B] = tuple._2.map(b => Map(tuple._1 -> b)).getOrElse(Map.empty)
  }

}
