package ru.sberbank.bigdata.enki.logger.journal

import org.apache.log4j.Logger

/** Logger used for any actions inside ENKI */
private[enki] trait EnkiLogging {

  private val enkiLogTitle = "ENKI"

  private val techPrefix = "[Tech]"

  private val infoPrefix = techPrefix + " [INFO] "

  private val warnPrefix = techPrefix + " [WARN] "

  private val errorPrefix = techPrefix + " [ERROR] "

  def info(message: String): Unit = logger.info(infoPrefix + EnkiLog(enkiLogTitle, className, message).toSingleLineJson)

  private[enki] def className: String = ""

  private[enki] def logger: Logger = Logger.getLogger(className)

  def warn(message: String): Unit = logger.warn(warnPrefix + EnkiLog(enkiLogTitle, className, message).toSingleLineJson)

  def error(message: String): Unit =
    logger.error(errorPrefix + EnkiLog(enkiLogTitle, className, message).toSingleLineJson)

  def error(message: String, t: Throwable): Unit =
    logger.error(errorPrefix + EnkiLog(enkiLogTitle, className, message).toSingleLineJson, t)

  def auditInfo(message: String): Unit = logger.info(message)

  def auditError(message: String, t: Throwable): Unit = logger.error(message, t)

}
