package ru.sberbank.bigdata.enki.framework.writer.partitions

import ru.sberbank.bigdata.enki.annotation.instantiate
import ru.sberbank.bigdata.enki.logger.journal.EnkiLogging

import scala.util.{Failure, Success, Try}
import scala.reflect.runtime.universe._

class PartitionsFromAnnotations extends EnkiLogging {

  def getClass[T: TypeTag] = symbolOf[T].asClass

  def extract[T: TypeTag]: Seq[String] =
    Try(getClass[T]) match {
      case Failure(exception) =>
        logger.warn(s"An $exception occurred during extraction partition columns from annotations")
        Seq.empty[String]
      case Success(klass) =>
        logger.info("Trying to get partition columns from annotations")
        klass.primaryConstructor.typeSignature.paramLists.head.map { symbol =>
          symbol.name.toString -> symbol.annotations
            .filter(_.tree.tpe <:< typeOf[PartitionAnnotation])
            .map(instantiate(_).asInstanceOf[PartitionAnnotation])
        }.collect {
          case (name, ann :: Nil) => name -> ann
          case (name, _ :: _) =>
            throw new Exception(s"Multiple partitions annotation on field $name of class ${klass.name.toString}.")
        }.sortBy(_._2.order)
          .map(_._1)
    }

}

object PartitionsFromAnnotations {
  def apply[T: TypeTag]: Seq[String] = (new PartitionsFromAnnotations).extract[T]
}
