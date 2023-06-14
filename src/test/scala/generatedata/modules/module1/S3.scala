package generatedata.modules.module1

import generatedata.modules.module1.struct.{TableScheme1, TableScheme2, TableScheme3}
import generatedata.modules.Tables.{getter1, getter2, getter3}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ru.sberbank.bigdata.enki.framework.{DerivedTable, WorkflowTask}
import org.apache.spark.sql.functions._

abstract class S3 extends DerivedTable[Row] {

  override val structMap: Option[Map[String, String]] = Some(
    Map(
      ("id", "int"),
      ("name", "string"),
      ("value", "double"),
      ("S1value", "long"),
      ("getTable1", "long")
    )
  )

  protected def getTable1(implicit spark: SparkSession): Dataset[TableScheme1]
  protected def getTable2(implicit spark: SparkSession): Dataset[TableScheme2]
  protected def getS1(implicit spark: SparkSession): Dataset[Row]
  protected def getS2(implicit spark: SparkSession): Dataset[Row]

  def gen(implicit spark: SparkSession): Dataset[Row] = {

    val result1 = getTable1
      .withColumn("getTable1", monotonically_increasing_id)
      .select("id", "name", "value", "getTable1")

    val result2 = getS2
      .withColumn("value3", col("S1value") * 10)
      .select("id", "name", "value", "S1value")

    val result3 = result1
      .join(result2, usingColumn = "id")

    result3
  }
}

object S3 extends S3 {
  override def className: String = "S3"
  override def schema: String    = "S3Scheme"
  override def name: String      = "S3Name"
  override def project: String   = "MyProject"

  override protected def getTable1(implicit spark: SparkSession): Dataset[TableScheme1] = getter1.get
  override protected def getTable2(implicit spark: SparkSession): Dataset[TableScheme2] = getter2.get
  override def getS1(implicit spark: SparkSession): Dataset[Row]                        = S1.get
  override def getS2(implicit spark: SparkSession): Dataset[Row]                        = S2.get
  override def dependencies: Seq[WorkflowTask[_]]                                       = Seq(S2)
}
