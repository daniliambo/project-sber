package ru.sberbank.bigdata.enki.logger

import org.apache.log4j.Logger
import sttp.client.Identity

final class IdentityLog4Enki(logger: Logger) extends Log4Enki[Identity] {
  override def isTraceEnabled: Identity[Boolean] = logger.isTraceEnabled

  override def isDebugEnabled: Identity[Boolean] = logger.isDebugEnabled

  override def isInfoEnabled: Identity[Boolean] = logger.isInfoEnabled

  override def error(message: => String): Identity[Unit] = logger.error(message)

  override def warn(message: => String): Identity[Unit] = logger.warn(message)

  override def info(message: => String): Identity[Unit] = logger.info(message)

  override def trace(message: => String): Identity[Unit] = logger.trace(message)

  override def debug(message: => String): Identity[Unit] = logger.debug(message)

  override def error(t: Throwable)(message: => String): Identity[Unit] = logger.error(message, t)

  override def warn(t: Throwable)(message: => String): Identity[Unit] = logger.warn(message, t)

  override def info(t: Throwable)(message: => String): Identity[Unit] = logger.info(message, t)

  override def trace(t: Throwable)(message: => String): Identity[Unit] = logger.trace(message, t)

  override def debug(t: Throwable)(message: => String): Identity[Unit] = logger.debug(message, t)
}

object IdentityLog4Enki {
  def apply(logger: Logger): IdentityLog4Enki = new IdentityLog4Enki(logger)
}
