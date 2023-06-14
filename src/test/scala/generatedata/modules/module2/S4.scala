package generatedata.modules.module2

import generatedata.modules.module1.struct.{TableScheme1, TableScheme2, TableScheme3}
import generatedata.modules.Tables.{getter1, getter2, getter3}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ru.sberbank.bigdata.enki.framework.{DerivedTable, WorkflowTask}

abstract class S4 extends DerivedTable[Row] {

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

  protected def getS3(implicit spark: SparkSession): Dataset[Row]

  protected def getS4(implicit spark: SparkSession): Dataset[Row]

  protected def getS5(implicit spark: SparkSession): Dataset[Row]

  def gen(implicit spark: SparkSession): Dataset[Row] = {
    import org.apache.spark.sql.functions._

    val result1 = getTable1
      .withColumn("value2", lit("new_value"))
      .select("id", "name", "value", "value2")

    result1
  }
}

object S4 extends S4 {
  override def className: String                                                        = "S4"
  override protected def getTable1(implicit spark: SparkSession): Dataset[TableScheme1] = getter1.get

  override protected def getTable2(implicit spark: SparkSession): Dataset[TableScheme2] = getter2.get

  override protected def getTable3(implicit spark: SparkSession): Dataset[TableScheme3] = getter3.get

  override def schema: String = "Scheme"

  override def name: String = s"S4Name"

  override def dependencies: Seq[WorkflowTask[_]] = Seq(S1, S2, S5, getter1, getter2, getter3)

  override def project: String = "MyProject"

  override protected def getS1(implicit spark: SparkSession): Dataset[Row] = S1.get

  override protected def getS5(implicit spark: SparkSession): Dataset[Row] = S5.get

  override protected def getS2(implicit spark: SparkSession): Dataset[Row] = S2.get

  override protected def getS3(implicit spark: SparkSession): Dataset[Row] = ???

  override protected def getS4(implicit spark: SparkSession): Dataset[Row] = ???
}
