package generatedata.modules.module1

import generatedata.modules.module1.struct.{TableScheme1, TableScheme2, TableScheme3}
import generatedata.modules.Tables.{getter1, getter2, getter3}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import ru.sberbank.bigdata.enki.framework.{DerivedTable, WorkflowTask}

abstract class S2 extends DerivedTable[Row] {

  override val structMap: Option[Map[String, String]] = Some(
    Map(
      ("id", "integer"),
      ("name", "string"),
      ("value", "string"),
      ("S1value", "long")
    )
  )

  protected def getTable2(implicit spark: SparkSession): Dataset[TableScheme2]
  protected def getS1(implicit spark: SparkSession): Dataset[Row]

  def gen(implicit spark: SparkSession): Dataset[Row] = {
    import org.apache.spark.sql.functions._

    val result1 = getS1
//      .where(col("S1value") > 10)
      .select("id", "value", "name", "S1value")

    result1
  }
}

object S2 extends S2 {
  override def className: String = "S2"
  override def schema: String    = "S2Scheme"
  override def name: String      = "S2Name"
  override def project: String   = "MyProject"

  override protected def getTable2(implicit spark: SparkSession): Dataset[TableScheme2] = getter2.get
  override protected def getS1(implicit spark: SparkSession): Dataset[Row]              = S1.get
  override def dependencies: Seq[WorkflowTask[_]]                                       = Seq(S1, getter2)
}
