package ru.sberbank.bigdata.enki.framework

import org.apache.spark.sql.SparkSession

final case class Stage(id: String, action: SparkSession => Unit)
