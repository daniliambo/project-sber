package ru.sberbank.bigdata.enki.test

import org.apache.spark.sql.SparkSession

trait SparkHdfsTest {

  implicit protected val spark: SparkSession = LocalClusterSession.localSpark

}
