package ru.sberbank.bigdata.enki

package object localcluster {

  def withClassLoader0[R](action: => R)(cl: Option[ClassLoader]): R = cl match {
    case Some(loader) =>
      val defaultLoader = Thread.currentThread().getContextClassLoader
      Thread.currentThread().setContextClassLoader(loader)
      val res = action
      Thread.currentThread().setContextClassLoader(defaultLoader)
      res
    case None => action
  }

  def withClassLoader[T, R](action: T => R)(cl: Option[ClassLoader]): T => R =
    param =>
      cl match {
        case Some(loader) =>
          val defaultLoader = Thread.currentThread().getContextClassLoader
          Thread.currentThread().setContextClassLoader(loader)
          val res = action(param)
          Thread.currentThread().setContextClassLoader(defaultLoader)
          res
        case None => action(param)
      }

  def withClassLoader2[T, Q, R](action: (T, Q) => R)(cl: Option[ClassLoader]): (T, Q) => R =
    (param1, param2) =>
      cl match {
        case Some(loader) =>
          val defaultLoader = Thread.currentThread().getContextClassLoader
          Thread.currentThread().setContextClassLoader(loader)
          val res = action(param1, param2)
          Thread.currentThread().setContextClassLoader(defaultLoader)
          res
        case None => action(param1, param2)
      }

}
