package ru.sberbank.bigdata.enki.framework.writer

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import ru.sberbank.bigdata.enki.framework.writer.data.CompressionStorageFormat

final case class TableConfiguration(
  name: String,
  partitionColumns: Option[Seq[String]],
  repartitionColumns: Option[Seq[String]],
  repartitionQty: Option[Int],
  bucketByColumns: Option[Seq[String]],
  bucketBySortColumns: Option[Seq[String]],
  bucketByQty: Option[Int],
  sortInPartitionColumns: Option[Seq[String]],
  maxRecordsPerFile: Option[Long],
  compressStoreFormat: Option[CompressionStorageFormat]
) {

  def getBucketSpec: Option[BucketSpec] =
    (bucketByColumns, bucketBySortColumns, bucketByQty) match {
      case (Some(cols), Some(sortCols), Some(qty)) =>
        Some(BucketSpec(numBuckets = qty, bucketColumnNames = cols, sortColumnNames = sortCols))
      case _ => None
    }
}
