package generatedata.writetables

import ru.sberbank.bigdata.enki.plan.nodes.SourceTableNode

import scala.tools.nsc.io
import scala.tools.nsc.io.File


trait PathGenerator {
  def generatePath(table: SourceTableNode): String ={
    //os specific separators
    val path = s"..target/SourceTables/${table.fullName}"
    path
  }

}
