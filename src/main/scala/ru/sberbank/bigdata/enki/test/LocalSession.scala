package ru.sberbank.bigdata.enki.test

import java.nio.file.{Files, Path}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/** Local session for Testing Views on local PC */
object LocalSession {
  lazy val warehouseDir: Path = Files.createTempDirectory("spark-warehouse")

  def localSpark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    SparkSession
      .builder()
      .appName("test")
      .master("local")
      .config("spark.sql.shuffle.partitions", 4)
      .config("spark.sql.warehouse.dir", warehouseDir.toUri.toString)
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()
  }
}
