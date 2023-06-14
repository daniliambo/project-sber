package generatedata.modules.module1

import generatedata.modules.module1.struct.{TableScheme1, TableScheme2, TableScheme3}
import generatedata.modules.Tables.{getter1, getter2, getter3}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ru.sberbank.bigdata.enki.framework.{DerivedTable, WorkflowTask}
import org.apache.spark.sql.functions._

abstract class S1 extends DerivedTable[Row] {

  override val structMap: Option[Map[String, String]] = Some(
    Map(
      ("id", "integer"),
      ("name", "string"),
      ("value", "string"),
      ("S1value", "long")
    )
  )

  protected def getTable1(implicit spark: SparkSession): Dataset[TableScheme1]

  def gen(implicit spark: SparkSession): Dataset[Row] = {

    val result1 = getTable1
      .withColumn("S1value", monotonically_increasing_id)
      .select("id", "name", "value", "S1value")
    result1
  }
}

object S1 extends S1 {
  override def className: String = "S1"
  override def schema: String    = "S1Scheme"
  override def name: String      = "S1Name"
  override def project: String   = "MyProject"

  override protected def getTable1(implicit spark: SparkSession): Dataset[TableScheme1] = getter1.get
  override def dependencies: Seq[WorkflowTask[_]]                                       = Seq(getter1)
}
