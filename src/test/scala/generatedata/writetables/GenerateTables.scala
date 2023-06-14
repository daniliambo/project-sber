package generatedata.writetables

import org.apache.spark.sql.DataFrame
import ru.sberbank.bigdata.enki.plan.nodes.SourceTableNode
import ru.sberbank.bigdata.enki.test.LocalSession

import scala.collection.mutable

trait GenerateTables extends PathGenerator {
  def generateTables(mapOfGeneratedData: mutable.Map[SourceTableNode, DataFrame]): Unit = {
    mapOfGeneratedData.foreach(x => {
      val path = generatePath(x._1)
      x._2
        .write
        .option("header", "true")
        .csv(path)
    })

  }

}
