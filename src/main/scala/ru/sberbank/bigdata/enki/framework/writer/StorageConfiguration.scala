package ru.sberbank.bigdata.enki.framework.writer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import pureconfig.error.ConfigReaderFailures
import pureconfig.ConfigSource
/* Used to fix error knownDirectSubclasses of <class> observed before subclass <class> registered
 * subclasses of CompressionStorageFormat must be compiled before knownDirectSubclasses gets hit */
import ru.sberbank.bigdata.enki.framework.writer.data.CompressionStorageFormat._
import ru.sberbank.bigdata.enki.framework.writer.data.CompressionStorageFormat
import ru.sberbank.bigdata.enki.framework.{DerivedTable, SparkConfigOverrides}

trait StorageConfiguration[T] {
  this: DerivedTable[T] =>

  private[this] val simpleConfig: Either[ConfigReaderFailures, SchemaConfiguration] = {
    import pureconfig.generic.auto._
    ConfigSource.default.load[SchemaConfiguration]
  }

  private[this] val settingsByTable: Map[String, TableConfiguration] = simpleConfig match {
    case Left(_) => Map.empty[String, TableConfiguration]
    case Right(config) =>
      config.tables.map { table =>
        (table.name, table)
      }.toMap
  }

  val newSettings: Option[TableConfiguration] = settingsByTable.get(name)

  private[this] def numPartitionsReconfigured(implicit spark: SparkSession): Option[Int] = {
    val res = if (newSettings.nonEmpty) newSettings.get.repartitionQty else None
    res.orElse(this.numPartitions).orElse(this.enkiNumPartitions)
  }

  private[this] def repartitionColNamesReconfigured: Seq[String] = {
    val res = if (newSettings.nonEmpty) newSettings.get.repartitionColumns else None
    res.getOrElse(this.repartitionColNames)
  }

  private[this] def sortColumnsReconfigured: Option[Seq[String]] = {
    val res = if (newSettings.nonEmpty) newSettings.get.sortInPartitionColumns else None
    res.orElse(this.sortColumns)
  }

  private[this] def bucketSpecReconfigured: Option[BucketSpec] = {
    val res = if (newSettings.nonEmpty) newSettings.get.getBucketSpec else None
    res.orElse(this.bucketSpec)
  }

  private[this] def partitionColNamesReconfigured: Seq[String] = {
    val res = if (newSettings.nonEmpty) newSettings.get.partitionColumns else None
    res.getOrElse(this.partitionColNames)
  }

  private[this] def compressStoreReconfigured: CompressionStorageFormat = {
    val res = if (newSettings.nonEmpty) newSettings.get.compressStoreFormat else None
    res.getOrElse(this.codecDataFormat)
  }

  private[this] def maxRecordsPerFileReconfigured: Option[Long] = {
    val res = if (newSettings.nonEmpty) newSettings.get.maxRecordsPerFile else None
    res.orElse(this.maxRecordsPerFile)
  }

  override def writerSettings(implicit spark: SparkSession): TableStoreWriteSetting = TableStoreWriteSetting(
    sparkConfigOverrides = SparkConfigOverrides.empty,
    repartitionBeforeSave = RepartitionSettings(partitionColumns = repartitionColNamesReconfigured,
                                                numPartitions = numPartitionsReconfigured
    ),
    maxRecordsPerFile = maxRecordsPerFileReconfigured,
    sortInPartitionBy = sortColumnsReconfigured,
    bucketBy          = bucketSpecReconfigured,
    partitionBy       = partitionColNamesReconfigured,
    compressStore     = compressStoreReconfigured
  )
}
