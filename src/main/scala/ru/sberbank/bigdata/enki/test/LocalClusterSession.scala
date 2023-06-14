package ru.sberbank.bigdata.enki.test

import java.nio.file.{Files, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import ru.sberbank.bigdata.enki.logger.journal.EnkiLogging

object LocalClusterSession extends EnkiLogging {
  lazy val warehouseDir: Path = Files.createTempDirectory("spark-warehouse")

  lazy val localSpark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.WARN)

    info(s"Using local spark session with HDFS support enabled")

    val session = SparkSession
      .builder()
      .appName("local-cluster")
      .master("local[*]")
      .config("spark.driver.host", "localhost")
      .config("spark.submit.deployMode", "cluster")
      .config("spark.hadoop.fs.defaultFS", LocalHdfsCluster.getDefaultFS)
      .config("spark.sql.warehouse.dir", warehouseDir.toUri.toString)
      .config("spark.sql.codegen.wholeStage", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .enableHiveSupport()
      .getOrCreate()

    info(s"Cluster nameservice is: ${session.sparkContext.hadoopConfiguration.get("fs.defaultFS")}")

    session

  }
}
