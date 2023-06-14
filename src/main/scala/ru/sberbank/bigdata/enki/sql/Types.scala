package ru.sberbank.bigdata.enki.sql

import java.sql.Timestamp

import org.apache.spark.sql.types._

object Types {

  trait TypeMapping[T] {
    def dataType: DataType
  }

  implicit object BigDecimalMapping extends TypeMapping[BigDecimal] {
    override def dataType: DataType = DecimalType.SYSTEM_DEFAULT
  }

  implicit object BooleanMapping extends TypeMapping[Boolean] {
    override def dataType: DataType = BooleanType
  }

  implicit object IntMapping extends TypeMapping[Int] {
    override def dataType: DataType = IntegerType
  }

  implicit object LongMapping extends TypeMapping[Long] {
    override def dataType: DataType = LongType
  }

  implicit object StringMapping extends TypeMapping[String] {
    override def dataType: DataType = StringType
  }

  implicit object TimestampMapping extends TypeMapping[Timestamp] {
    override def dataType: DataType = TimestampType
  }

}
