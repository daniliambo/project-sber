package ru.sberbank.bigdata.enki.logger

import org.apache.log4j.Logger
import zio.Task

final class ZIOLog4Enki(logger: Logger) extends Log4Enki[Task] {
  override def isTraceEnabled: Task[Boolean] = Task(logger.isTraceEnabled)

  override def isDebugEnabled: Task[Boolean] = Task(logger.isDebugEnabled)

  override def isInfoEnabled: Task[Boolean] = Task(logger.isInfoEnabled)

  override def error(message: => String): Task[Unit] = Task(logger.error(message))

  override def warn(message: => String): Task[Unit] = Task(logger.warn(message))

  override def info(message: => String): Task[Unit] = isInfoEnabled.flatMap(Task(logger.info(message)).when(_))

  override def trace(message: => String): Task[Unit] = isTraceEnabled.flatMap(Task(logger.trace(message)).when(_))

  override def debug(message: => String): Task[Unit] = isDebugEnabled.flatMap(Task(logger.debug(message)).when(_))

  override def error(t: Throwable)(message: => String): Task[Unit] = Task(logger.error(message, t))

  override def warn(t: Throwable)(message: => String): Task[Unit] = Task(logger.warn(message, t))

  override def info(t: Throwable)(message: => String): Task[Unit] =
    isInfoEnabled.flatMap(Task(logger.info(message, t)).when(_))

  override def trace(t: Throwable)(message: => String): Task[Unit] =
    isTraceEnabled.flatMap(Task(logger.trace(message, t)).when(_))

  override def debug(t: Throwable)(message: => String): Task[Unit] =
    isDebugEnabled.flatMap(Task(logger.debug(message, t)).when(_))
}

object ZIOLog4Enki {
  def apply(logger: Logger): ZIOLog4Enki = new ZIOLog4Enki(logger)
}
