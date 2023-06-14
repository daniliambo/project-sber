package ru.sberbank.bigdata.enki.framework

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ru.sberbank.bigdata.enki.framework.writer.StorageConfiguration
import ru.sberbank.bigdata.enki.schema.withMetadataFromAnnotations

import scala.reflect.runtime.universe._

abstract class DerivedTable[T: TypeTag] extends Table[T] with DerivedView[T] with StorageConfiguration[T] {
  def gen(implicit spark: SparkSession): Dataset[T]

  def genWithMetadata(implicit spark: SparkSession): Dataset[T] =
    if (typeOf[T] == typeOf[Row]) {
      gen
    } else {
      withMetadataFromAnnotations(gen)
    }

  def fileSystemDelete(path: Path, recursive: Boolean)(implicit spark: SparkSession): Unit =
    AuditDelete(fileSystem.delete(path, recursive), path.toString)

  def sparkSqlDrop(tableName: String)(implicit spark: SparkSession): Unit =
    AuditDelete(
      spark.sql(s"drop table if exists $tableName"),
      tableName
    )

  override def execute(args: Array[String])(implicit spark: SparkSession): Dataset[T] = gen

  override def get(implicit spark: SparkSession): Dataset[T] =
    if (persist) {
      read(TableLocation(qualifiedName, path))
    } else {
      gen
    }

  override def refresh(implicit spark: SparkSession): Boolean =
    if (persist) {
      // In Spark 2.4+ table creation inside .saveAsTable method in overwrite mode
      // sometimes might fail with AlreadyExistsException, so we need to manually
      // drop table and delete files from path if any
      delete

      // Из-за использования кеширования и spark actions в gen могут выполняться запросы.
      // Здесь применяется конфигурация из writer settings перед вызовом gen.
      val dataset = withSparkConfig(gen)
      write(Overwrite, dataset)
      true
    } else {
      info(s"Refresh action skipped, persist flag set to false for $qualifiedName.")
      false
    }

  override def delete(implicit spark: SparkSession): Unit = if (persist) {
    sparkSqlDrop(qualifiedName)

    path.foreach { p: String =>
      val tblPath = new Path(p)
      if (fileSystem.exists(tblPath))
        fileSystemDelete(tblPath, recursive = true)
    }
  }
}
