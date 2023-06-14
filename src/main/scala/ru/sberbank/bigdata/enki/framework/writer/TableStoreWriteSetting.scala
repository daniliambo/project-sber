package ru.sberbank.bigdata.enki.framework.writer

import ru.sberbank.bigdata.enki.framework.writer.data.CompressionStorageFormat
/* Used to fix error knownDirectSubclasses of <class> observed before subclass <class> registered
 * subclasses of CompressionStorageFormat must be compiled before knownDirectSubclasses
 * in 'io.circe.generic.auto._' gets hit */
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import ru.sberbank.bigdata.enki.framework.SparkConfigOverrides

final case class TableStoreWriteSetting(
  sparkConfigOverrides: SparkConfigOverrides,
  repartitionBeforeSave: RepartitionSettings,
  maxRecordsPerFile: Option[Long],
  partitionBy: Seq[String],
  sortInPartitionBy: Option[Seq[String]],
  bucketBy: Option[BucketSpec],
  compressStore: CompressionStorageFormat
) {

  def withRepartition(f: RepartitionSettings => RepartitionSettings): TableStoreWriteSetting =
    this.copy(repartitionBeforeSave = f(repartitionBeforeSave))

  def withSparkConfig(f: SparkConfigOverrides => SparkConfigOverrides): TableStoreWriteSetting =
    this.copy(sparkConfigOverrides = f(sparkConfigOverrides))

  def toMultiLineJson: String = this.asJson.spaces2

  def toSingleLineJson: String = this.asJson.noSpaces

}
