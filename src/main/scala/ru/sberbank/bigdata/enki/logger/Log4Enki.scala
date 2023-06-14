package ru.sberbank.bigdata.enki.logger

/** Enki implementation of log4cats
  * Because log4cats does not support scala 2.11
  * when enki stops using 2.11, this trait and all it's successors will become obsolete
  */
trait Log4Enki[F[_]] {

  def isTraceEnabled: F[Boolean]
  def isDebugEnabled: F[Boolean]
  def isInfoEnabled: F[Boolean]

  def error(message: => String): F[Unit]
  def warn(message: => String): F[Unit]
  def info(message: => String): F[Unit]
  def trace(message: => String): F[Unit]
  def debug(message: => String): F[Unit]

  def error(t: Throwable)(message: => String): F[Unit]
  def warn(t: Throwable)(message: => String): F[Unit]
  def info(t: Throwable)(message: => String): F[Unit]
  def trace(t: Throwable)(message: => String): F[Unit]
  def debug(t: Throwable)(message: => String): F[Unit]

}
