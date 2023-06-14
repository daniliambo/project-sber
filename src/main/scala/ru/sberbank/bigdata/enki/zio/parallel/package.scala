package ru.sberbank.bigdata.enki.zio

import zio.Task

package object parallel {

  def parallelTask[T, R](values: Seq[T], f: T => R, parallelism: Int): Task[Seq[R]] =
    Task.collectAllParN(parallelism)(values.map(v => Task(f(v))))
}
