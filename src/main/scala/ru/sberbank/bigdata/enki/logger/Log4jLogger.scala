package ru.sberbank.bigdata.enki.logger

import cats.effect.Sync
import org.apache.log4j.Logger
import cats.implicits._

final class Log4jLogger[F[_]](logger: Logger)(implicit F: Sync[F]) extends Log4Enki[F] {

  override def isTraceEnabled: F[Boolean] = F.delay(logger.isTraceEnabled)

  override def isDebugEnabled: F[Boolean] = F.delay(logger.isDebugEnabled)

  override def isInfoEnabled: F[Boolean] = F.delay(logger.isInfoEnabled)

  override def error(message: => String): F[Unit] = F.delay(logger.error(message))

  override def warn(message: => String): F[Unit] = F.delay(logger.warn(message))

  override def info(message: => String): F[Unit] = isInfoEnabled.ifM(F.delay(logger.info(message)), F.unit)

  override def trace(message: => String): F[Unit] = isTraceEnabled.ifM(F.delay(logger.trace(message)), F.unit)

  override def debug(message: => String): F[Unit] = isDebugEnabled.ifM(F.delay(logger.debug(message)), F.unit)

  override def error(t: Throwable)(message: => String): F[Unit] = F.delay(logger.error(message, t))

  override def warn(t: Throwable)(message: => String): F[Unit] = F.delay(logger.warn(message, t))

  override def info(t: Throwable)(message: => String): F[Unit] =
    isInfoEnabled.ifM(F.delay(logger.info(message, t)), F.unit)

  override def trace(t: Throwable)(message: => String): F[Unit] =
    isInfoEnabled.ifM(F.delay(logger.trace(message, t)), F.unit)

  override def debug(t: Throwable)(message: => String): F[Unit] =
    isInfoEnabled.ifM(F.delay(logger.debug(message, t)), F.unit)

}

object Log4jLogger {
  def apply[F[_]](logger: Logger)(implicit F: Sync[F]): Log4jLogger[F] = new Log4jLogger(logger)(F)
}
