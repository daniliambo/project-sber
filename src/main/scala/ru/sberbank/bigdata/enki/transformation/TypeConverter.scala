package ru.sberbank.bigdata.enki.transformation

import org.apache.spark.sql.Column
import org.apache.spark.sql.types.DataType

final private[transformation] case class TypeConverter(from: DataType, to: DataType, convert: Column => Column)
