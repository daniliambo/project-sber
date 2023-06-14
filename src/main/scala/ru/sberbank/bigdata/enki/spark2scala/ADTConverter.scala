package ru.sberbank.bigdata.enki.spark2scala

import org.apache.spark.sql.types._
import ru.sberbank.bigdata.enki.spark2scala.exceptions.UnsupportedConversion

final class ADTConverter {

  private def escape(fieldName: String): String =
    fieldName match {
      case "class" | "type" | "val" | "import" | "case" => '`' + fieldName + '`'
      case name if name.endsWith("_")                   => name + " "
      case _                                            => fieldName
    }

  def buildScalaClasses(mainClassName: String, schema: StructType): String = {
    def go(className: String, schema: StructType): String = {
      val fields = schema.fields
      val structs =
        fields.collect {
          case StructField(name, dataType: StructType, _, _) => go(s"${className}_$name", dataType)
          case StructField(name, dataType: ArrayType, _, _) =>
            dataType.elementType match {
              case elementType: StructType => go(s"${className}_$name", elementType)
              case _                       => ""
            }
          case StructField(name, dataType: MapType, _, _) =>
            val keyStruct = dataType.keyType match {
              case keyType: StructType => go(s"${className}_${name}_Key", keyType)
              case _                   => ""
            }
            val valueStruct = dataType.valueType match {
              case valueType: StructType => go(s"${className}_${name}_Value", valueType)
              case _                     => ""
            }
            keyStruct + valueStruct
        }.mkString

      val body = fields.map {
        case field @ StructField(name, _: StructType, _, _) =>
          (field, s"${(s"${className}_$name")}")
        case field @ StructField(name, arrayType: ArrayType, _, _) =>
          arrayType.elementType match {
            case _: StructType =>
              if (arrayType.containsNull) {
                (field, s"scala.collection.Seq[Option[${(s"${className}_$name")}]]")
              } else {
                (field, s"scala.collection.Seq[${(s"${className}_$name")}]")
              }
            case _ => (field, sparkFieldToScala(arrayType))
          }
        case field @ StructField(name, mapType: MapType, _, _) =>
          val key = mapType.keyType match {
            case _: StructType => (s"${className}_${name}_Key")
            case _             => sparkFieldToScala(mapType.keyType)
          }
          val value = mapType.valueType match {
            case _: StructType => (s"${className}_${name}_Value")
            case _             => sparkFieldToScala(mapType.valueType)
          }
          if (mapType.valueContainsNull)
            (field, s"scala.collection.Map[$key, Option[$value]]")
          else
            (field, s"scala.collection.Map[$key, $value]")
        case field @ StructField(_, dataType, _, _) => (field, s"${sparkFieldToScala(dataType)}")
      }
        .map(x => s"${escape(x._1.name)}: ${scalaOptional(x._1, x._2)}")
        .mkString(", ")

      s"""$structs
         |final case class ${(className)}($body)
         |""".stripMargin
    }

    go(mainClassName, schema)
  }

  def buildScalaClass(name: String, schema: StructType): String = {
    val body = schema.map(field => s"${escape(field.name)}: ${sparkFieldToScala(field.dataType)}").mkString(", ")
    s"final case class $name($body)"
  }

  def sparkFieldToScala(dataType: DataType): String = dataType match {
    case ByteType       => "Byte"
    case ShortType      => "Short"
    case IntegerType    => "Int"
    case LongType       => "Long"
    case FloatType      => "Float"
    case DoubleType     => "Double"
    case _: DecimalType => "BigDecimal"
    case StringType     => "String"
    case BinaryType     => "Array[Byte]"
    case BooleanType    => "Boolean"
    case TimestampType  => "java.sql.Timestamp"
    case DateType       => "java.sql.Date"
    case ArrayType(keyType, containsNull) =>
      if (containsNull) s"scala.collection.Seq[Option[${sparkFieldToScala(keyType)}]]"
      else s"scala.collection.Seq[${sparkFieldToScala(keyType)}]"
    case MapType(keyType, valueType, valueContainsNull) =>
      if (valueContainsNull)
        s"scala.collection.Map[${sparkFieldToScala(keyType)}, Option[${sparkFieldToScala(valueType)}]]"
      else s"scala.collection.Map[${sparkFieldToScala(keyType)}, ${sparkFieldToScala(valueType)}]"
    case _ => throw UnsupportedConversion(None)
  }

  def scalaOptional(field: StructField, scalaADT: String): String = if (field.nullable) {
    s"Option[$scalaADT]"
  } else {
    scalaADT
  }
}
