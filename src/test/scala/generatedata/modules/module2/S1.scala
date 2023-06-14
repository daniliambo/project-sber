package generatedata.modules.module2

import generatedata.modules.module1.struct.{TableScheme1, TableScheme2, TableScheme3}
import generatedata.modules.Tables.{getter1, getter2, getter3}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ru.sberbank.bigdata.enki.framework.{DerivedTable, WorkflowTask}

abstract class S1 extends DerivedTable[Row] {

  override val structMap: Option[Map[String, String]] = Some(
    Map(
      ("name", "string"),
      ("id", "integer"),
      ("value", "string"),
      ("value2", "string"),
      ("StringType", "string"),
      ("BooleanType", "boolean"),
      ("ByteType", "byte"),
      ("ShortType", "short"),
      ("IntegerType", "integer"),
      ("LongType", "long"),
      ("VarcharType", "varchar(32)"),
      ("CharType", "char(32)"),
      ("FloatType", "float"),
      ("DoubleType", "double"),
      ("TimestampType", "timestamp"),
      ("DateType", "date"),
      ("DecimalType", "Decimal(38,18)"),
      ("StructType", "Struct<id: int, name: string, value: double>"),
      ("ArrayType", "Array<string>"),
      ("MapType", "Map<string, string>")
    )
  )

  protected def getTable1(implicit spark: SparkSession): Dataset[TableScheme1]

  protected def getTable2(implicit spark: SparkSession): Dataset[TableScheme2]

  protected def getTable3(implicit spark: SparkSession): Dataset[TableScheme3]

  def gen(implicit spark: SparkSession): Dataset[Row] = {
    import org.apache.spark.sql.functions._

    val result1 = getTable1
      .withColumn("value2", lit("new_value"))
      .select(
        "id",
        "name",
        "value",
        "value2",
        "StringType",
        "BooleanType",
        "ByteType",
        "ShortType",
        "IntegerType",
        "LongType",
        "VarcharType",
        "CharType",
        "FloatType",
        "DoubleType",
        "TimestampType",
        "DateType",
        "StructType",
        "DecimalType",
        "ArrayType",
        "MapType"
      )
    result1
  }
}

object S1 extends S1 {
  override def className: String                                                        = "S1"
  override protected def getTable1(implicit spark: SparkSession): Dataset[TableScheme1] = getter1.get

  override protected def getTable2(implicit spark: SparkSession): Dataset[TableScheme2] = getter2.get

  override protected def getTable3(implicit spark: SparkSession): Dataset[TableScheme3] = getter3.get

  override def schema: String = "Scheme"

  override def name: String = s"S1Name"

  override def dependencies: Seq[WorkflowTask[_]] = Seq(getter1, getter2, getter3)

  override def project: String = "MyProject"
}
