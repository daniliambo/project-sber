package ru.sberbank.bigdata.enki.test

import org.apache.spark.sql.SparkSession
import ru.sberbank.bigdata.enki.test.LocalSession.localSpark

trait SparkContextSetup {

  def withSparkContext(testmethod: (SparkSession) => Any): Unit = {
    val spark = localSpark
    try testmethod(spark)
    finally spark.stop()
  }

}
