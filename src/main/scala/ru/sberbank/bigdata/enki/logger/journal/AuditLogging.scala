package ru.sberbank.bigdata.enki.logger.journal

import ru.sberbank.bigdata.enki.logger.DataExistingOperation
import java.net._
import scala.util.{Failure, Success, Try}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

private[enki] trait AuditLogging extends EnkiLogging {
  def AuditRead[B](func: => B, objectName: String): B = func

  def AuditWrite[B](func: => B, objectName: String): B = func

  def AuditDelete[B](func: => B, objectName: String): B = func

}
