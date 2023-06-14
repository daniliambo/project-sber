package generatedata.modules.module2

import generatedata.modules.module1.struct.{TableScheme1, TableScheme2, TableScheme3}
import generatedata.modules.Tables.{getter1, getter2, getter3}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ru.sberbank.bigdata.enki.framework.{DerivedTable, WorkflowTask}

abstract class S3 extends DerivedTable[Row] {

  override val structMap: Option[Map[String, String]] = Some(
    Map(
      ("id", "int"),
      ("name", "string"),
      ("value", "double"),
      ("value2", "string"),
      ("value3", "string")
    )
  )

  protected def getTable1(implicit spark: SparkSession): Dataset[TableScheme1]

  protected def getTable2(implicit spark: SparkSession): Dataset[TableScheme2]

  protected def getTable3(implicit spark: SparkSession): Dataset[TableScheme3]

  protected def getS1(implicit spark: SparkSession): Dataset[Row]

  protected def getS2(implicit spark: SparkSession): Dataset[Row]

  def gen(implicit spark: SparkSession): Dataset[Row] = {
    import org.apache.spark.sql.functions._

    val result1 = getTable1
      .withColumn("value2", lit("new_value"))
      .select("id", "name", "value", "value2")

    val result2 = getS1
      .withColumn("value2", lit("new_value"))
      .select("id", "name", "value", "value2")

    val result3 = getS2
      .withColumn("value3", lit("new_value"))
      .select("id", "name", "value", "value3")

    val result4 = result3
      .join(result1, usingColumn = "id")
      .join(result2, result2("id") === result3("id"))
      .select(result2("id"), result3("name"), result2("value"), result2("value2"), result3("value3"))

    result4
  }
}

object S3 extends S3 {
  override def className: String                                                        = "S3"
  override protected def getTable1(implicit spark: SparkSession): Dataset[TableScheme1] = getter1.get

  override protected def getTable2(implicit spark: SparkSession): Dataset[TableScheme2] = getter2.get

  override protected def getTable3(implicit spark: SparkSession): Dataset[TableScheme3] = getter3.get

  override def schema: String = "Scheme"

  override def name: String = s"S3Name"

  override def dependencies: Seq[WorkflowTask[_]] = Seq(S1, S2, getter1, getter2, getter3)

  override def project: String = "MyProject"

  // get - получить DataSet
  // gen - вычислить значения
  override def getS1(implicit spark: SparkSession): Dataset[Row] = S1.get

  override def getS2(implicit spark: SparkSession): Dataset[Row] = S2.get
}
