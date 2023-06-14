package ru.sberbank.bigdata.enki.framework.writer

import org.apache.spark.sql.catalyst.catalog.BucketSpec

trait Bucketed {

  def bucketSpec: Option[BucketSpec] = None

}
