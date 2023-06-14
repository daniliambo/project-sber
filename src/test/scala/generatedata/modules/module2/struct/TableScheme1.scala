package generatedata.modules.module2.struct

import org.apache.spark.sql.types.DecimalType

import java.sql.{Date, Timestamp}

final case class TableScheme1(
  id: Option[Int],
  name: String,
  value: String,
  StringType: String,
  BooleanType: Boolean,
  ByteType: Byte,
  ShortType: Short,
  IntegerType: Integer,
  LongType: Long,
  VarcharType: String,
  CharType: String,
  FloatType: Float,
  DoubleType: Double,
  TimestampType: Timestamp,
  DateType: Date,
  StructType: TableScheme2,
  DecimalType: DecimalType,
  ArrayType: Array[String],
  MapType: Map[String, String]
)
