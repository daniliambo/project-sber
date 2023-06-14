package ru.sberbank.bigdata.enki

import org.apache.spark.sql.SparkSession
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailures
import ru.sberbank.bigdata.enki.framework.DerivedView
import ru.sberbank.bigdata.enki.framework.extensions.ArgumentExtensions
import ru.sberbank.bigdata.enki.logger.journal.EnkiLogging

package object datagen extends EnkiLogging {

  private[datagen] val configuration: Either[ConfigReaderFailures, SchemaPath] = {
    // format: off
    import pureconfig.generic.auto._
    // format: on
    ConfigSource.resources("datagen.conf").load[SchemaPath]
  }

  private[datagen] val schemaPath: Map[String, String] = configuration match {
    case Left(_) => Map.empty[String, String]
    case Right(config) =>
      config.databases.map { db =>
        (db._1, db._2)
      }
  }
  private[datagen] val nanoseconds = 1000 * 1000 * 1000L
  private[datagen] val defaultTs   = 1550 * nanoseconds

  def ensureDatabase(database: String)(implicit spark: SparkSession): Unit = {
    info(s"Running command: CREATE DATABASE IF NOT EXISTS $database LOCATION '${dbLocation(database)}' ...")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $database LOCATION '${dbLocation(database)}'")
  }

  private[datagen] def dbLocation(name: String) =
    schemaPath.getOrElse(
      name,
      throw new IllegalArgumentException(s"Location of the database $name wasn't specified in configuration")
    )

  def tableLocation(database: String, table: String): String =
    s"${dbLocation(database)}/$table"

  implicit object GeneratorsArgument extends ArgumentExtensions[Seq[String]]("generators") {
    override protected def parseUnsafe: String => Seq[String] = s => s.split(",")
  }

  implicit object ClassesArgument extends ArgumentExtensions[Seq[DerivedView[_]]]("classes") {

    override protected def parseUnsafe: String => Seq[DerivedView[_]] =
      _ => Seq.empty
  }

}
