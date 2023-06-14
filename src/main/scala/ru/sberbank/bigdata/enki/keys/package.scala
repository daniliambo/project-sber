package ru.sberbank.bigdata.enki

import ru.sberbank.bigdata.enki.annotation.instantiate

import scala.reflect.runtime.universe._

//TODO: переместить во  framework?
package object keys {

  def primaryKeyFromAnnotation[T: TypeTag]: Seq[String] = {
    val klass = symbolOf[T].asClass
    klass.primaryConstructor.typeSignature.paramLists.head.map { symbol =>
      symbol.name.toString -> symbol.annotations
        .filter(_.tree.tpe <:< typeOf[KeyAnnotation])
        .map(instantiate(_).asInstanceOf[KeyAnnotation])
    }.collect {
      case (name, ann :: Nil) => name -> ann
      case (name, ann :: _) =>
        throw new Exception(s"Multiple primary key annotation on field $name of class ${klass.name.toString}.")
    }.sortBy(_._2.order)
      .map(_._1)
  }
}
