package ru.sberbank.bigdata.enki.framework

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.{DataFrameWriter, _}
import ru.sberbank.bigdata.enki.framework.writer.data.CompressedStored
import ru.sberbank.bigdata.enki.framework.writer._
import ru.sberbank.bigdata.enki.framework.writer.data.DataStorageFormat.Csv
import ru.sberbank.bigdata.enki.keys.KeyFromTag
import ru.sberbank.bigdata.enki.framework.writer.partitions.PartitionsFromTag
import ru.sberbank.bigdata.enki.logger.journal.{AuditLogging, EnkiLogging}
import ru.sberbank.bigdata.enki.schema.withMetadataFromAnnotations

import scala.reflect.runtime.universe._
import scala.util.{Failure, Success, Try}

/** Hive table store default implementation.
  */
class TableStore[T: TypeTag]
    extends Store[T]
    with Encoded[T]
    with HasTag[T]
    with EncoderFromTag[T]
    with KeyFromTag[T]
    with PartitionsFromTag[T]
    with CompressedStored
    with Sorted
    with Bucketed
    with EnkiLogging
    with AuditLogging {

  override type Location = TableLocation

  def tag: TypeTag[T] = typeTag[T]

  implicit private[enki] def fileSystem(implicit spark: SparkSession): FileSystem =
    FileSystem.get(spark.sparkContext.hadoopConfiguration)

  def customOptions: Map[String, String] = if (codecDataFormat.dataFormat == Csv) csvOptions else Map()

  override def encode(data: Dataset[T])(implicit session: SparkSession): DataFrame =
    if (typeOf[T] == typeOf[Row]) {
      data.toDF
    } else {
      withMetadataFromAnnotations(data).toDF()
    }

  override def decode(dataFrame: DataFrame)(implicit session: SparkSession): Dataset[T] =
    dataFrame.as[T](encoder(dataFrame.schema))

  def writerSettings(implicit session: SparkSession): TableStoreWriteSetting = TableStoreWriteSetting(
    sparkConfigOverrides = SparkConfigOverrides.empty,
    repartitionBeforeSave =
      RepartitionSettings(partitionColumns = repartitionColNames, numPartitions = enkiNumPartitions),
    maxRecordsPerFile = maxRecordsPerFile,
    sortInPartitionBy = sortColumns,
    bucketBy          = bucketSpec,
    partitionBy       = partitionColNames,
    compressStore     = codecDataFormat
  )

  protected[enki] def withSparkConfig[R](action: => R)(implicit session: SparkSession): R = {
    val configOverrides = writerSettings.sparkConfigOverrides.toMap
    val savedConfig     = configOverrides.keys.flatMap(k => session.conf.getOption(k).map(v => k -> v))
    try {
      configOverrides.foreach(c => session.conf.set(c._1, c._2))
      action
    } finally savedConfig.foreach(c => session.conf.set(c._1, c._2))
  }

  protected[enki] def withWriter(
    path: Option[String]
  )(action: (Dataset[T] => DataFrameWriter[T]) => Unit)(implicit session: SparkSession): Unit = {

    def format(writer: DataFrameWriter[T]): DataFrameWriter[T] = {
      val defaultWriter = writer
        .format(writerSettings.compressStore.dataFormat.toString)
        .option("compression", writerSettings.compressStore.compression.toString)

      customOptions.foreach(x => defaultWriter.option(x._1, x._2))

      defaultWriter
    }

    def setPath(writer: DataFrameWriter[T]): DataFrameWriter[T] = path match {
      case Some(path) => writer.option("path", path)
      case None       => writer
    }

    def partitionBy(writer: DataFrameWriter[T]): DataFrameWriter[T] = writerSettings.partitionBy match {
      case Seq()    => writer
      case partCols => writer.partitionBy(partCols: _*)
    }

    def bucketBy(writer: DataFrameWriter[T]): DataFrameWriter[T] = writerSettings.bucketBy match {
      case Some(BucketSpec(numBuckets, bucketColumns, sortColumns)) =>
        val bucketed = writer.bucketBy(numBuckets, bucketColumns.head, bucketColumns.tail: _*)
        sortColumns match {
          case Seq()   => bucketed
          case x +: xs => bucketed.sortBy(x, xs: _*)
        }
      case None => writer
    }

    def repartition(data: Dataset[T]): Dataset[T] = writerSettings.repartitionBeforeSave match {
      case RepartitionSettings(Seq(), None)             => data
      case RepartitionSettings(Seq(), Some(partNum))    => data.repartition(partNum)
      case RepartitionSettings(colNames, None)          => data.repartition(colNames.map(data(_)): _*)
      case RepartitionSettings(colNames, Some(partNum)) => data.repartition(partNum, colNames.map(data(_)): _*)
    }

    def sortInPartition(data: Dataset[T]): Dataset[T] = writerSettings.sortInPartitionBy match {
      case Some(col +: Nil)  => data.sortWithinPartitions(col)
      case Some(col +: cols) => data.sortWithinPartitions(col, cols: _*)
      case Some(Nil) | None  => data
    }

    def setMaxRecordsPerFile(writer: DataFrameWriter[T]): DataFrameWriter[T] = writerSettings.maxRecordsPerFile match {
      case Some(maxRecords) => writer.option("maxRecordsPerFile", maxRecords)
      case None             => writer
    }

    withSparkConfig {
      action(data =>
        setMaxRecordsPerFile(setPath(format(bucketBy(partitionBy(sortInPartition(repartition(data)).write)))))
      )
    }
  }

  protected def exists(location: Location)(implicit session: SparkSession): Boolean =
    location.path match {
      case Some(path) =>
        if (true) {
          fileSystem.exists(new Path(path)) && fileSystem
            .isDirectory(new Path(path)) && fileSystem.getContentSummary(new Path(path)).getFileCount != 0
        } else {
          fileSystem.exists(new Path(path))
        }
      case None =>
        if (true) {
          session.catalog.tableExists(location.qualifiedName) && !session.table(location.qualifiedName).rdd.isEmpty()
        } else {
          session.catalog.tableExists(location.qualifiedName)
        }
    }

  protected def hasData(location: Location)(implicit session: SparkSession): Boolean = read(location).head(1).nonEmpty

  /** Checks if table has any data.
    *
    * @param location Table name with schema and its path in hdfs(optional)
    */
  protected def hasUntypedData(location: Location)(implicit session: SparkSession): Boolean =
    location.path match {
      case Some(path) =>
        Try(session.read.format(codecDataFormat.dataFormat.toString).load(path).head(1).nonEmpty) match {
          case Success(bool: Boolean) => bool
          case Failure(e) =>
            warn(
              s"Path $path for table ${location.qualifiedName} exists, but probably has no data, resulting in exception: '$e'"
            )
            false
        }
      case None => session.read.table(location.qualifiedName).head(1).nonEmpty
    }

  override def read(location: Location)(implicit session: SparkSession): Dataset[T] =
    AuditRead(
      decode(session.read.table(location.qualifiedName)),
      location.qualifiedName
    )

  def readDf(location: Location)(implicit session: SparkSession): DataFrame =
    AuditRead(
      session.read.table(location.qualifiedName),
      location.qualifiedName
    )

  override def write(location: Location, mode: WriteMode, data: Dataset[T])(implicit session: SparkSession): Unit =
    withWriter(location.path) { writer =>
      mode match {
        case Overwrite =>
          AuditWrite(writer(data)
                       .mode(SaveMode.Overwrite)
                       .saveAsTable(location.qualifiedName),
                     location.qualifiedName
          )
        case Append =>
          AuditWrite(writer(data)
                       .mode(SaveMode.Append)
                       .saveAsTable(location.qualifiedName),
                     location.qualifiedName
          )
      }
    }

  override def write(path: String, mode: WriteMode, data: Dataset[T])(implicit session: SparkSession): Unit =
    withWriter(Some(path)) { writer =>
      mode match {
        case Overwrite =>
          AuditWrite(writer(data)
                       .mode(SaveMode.Overwrite)
                       .save(path),
                     path
          )
        case Append =>
          AuditWrite(writer(data)
                       .mode(SaveMode.Append)
                       .save(path),
                     path
          )
      }
    }
}
