package generatedata.getters

import generatedata.utils.TableType
import org.apache.spark.sql.SparkSession
import ru.sberbank.bigdata.enki.ctl.dependencies.innerDependencies
import ru.sberbank.bigdata.enki.framework.{DerivedTable, Table, WorkflowExecutable}

trait GetTablesMethods {

  def getAllSteps(leaves: Seq[WorkflowExecutable[_]]): Seq[DerivedTable[_]] =
    innerDependencies(leaves).collect { case d: DerivedTable[_] => d }

  def createDatabase(database: String)(implicit sparkSession: SparkSession): Unit =
    sparkSession.sql(s"CREATE DATABASE IF NOT EXISTS $database").show

  def createTable(table: Table[_])(implicit sparkSession: SparkSession): Unit =
    table.getEmpty().write.saveAsTable(table.qualifiedName)

  def generateSchemesAndTables(sourceDeps: Set[TableType[_]])(implicit sparkSession: SparkSession): Unit = {
    sourceDeps.map(_.schema).foreach(createDatabase)
    sourceDeps.foreach(createTable)
  }
}
