package ru.sberbank.bigdata.enki

import org.apache.spark.sql.types.StructField
import ru.sberbank.bigdata.enki.annotation._
import ru.sberbank.bigdata.enki.framework.WorkflowTask

import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._

/** Metadata and metadata specific annotations. */
package object metadata {

  implicit class ViewExtension(view: WorkflowTask[_]) {
    private lazy val klass = currentMirror.classSymbol(view.getClass).toType.typeSymbol.asClass

    def getDisplayName: String =
      getAnnotation[displayName].map(_.value).getOrElse(klass.name.toString)

    private def getAnnotation[A <: MetadataAnnotation: TypeTag]: Option[MetadataAnnotation] =
      klass.baseClasses
        .flatMap(_.annotations)
        .collectFirst {
          case a if a.tree.tpe <:< typeOf[A] => instantiate(a).asInstanceOf[MetadataAnnotation]
        }

    def getDescription: String =
      getAnnotation[description].map(_.value).getOrElse("")
  }

  implicit class StructFieldExtension(field: StructField) {

    def getDisplayName: String =
      getString(MetadataKey.DisplayName).getOrElse(field.name)

    def getDescription: String =
      getString(MetadataKey.Description).getOrElse("")

    def isPrimaryKey: Boolean =
      getBoolean(MetadataKey.PrimaryKey).getOrElse(false)

    def isForeignKey: Boolean =
      getBoolean(MetadataKey.ForeignKey).getOrElse(false)

    private def getString(key: String): Option[String] =
      if (field.metadata.contains(key)) {
        Some(field.metadata.getString(key))
      } else {
        None
      }

    private def getBoolean(key: String): Option[Boolean] =
      if (field.metadata.contains(key)) {
        Some(field.metadata.getBoolean(key))
      } else {
        None
      }
  }

}
