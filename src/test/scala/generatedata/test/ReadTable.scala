package generatedata.test

import ru.sberbank.bigdata.enki.test.LocalSession

trait ReadTable {

  def readTable(path: String): Unit = {
    val spark = LocalSession.localSpark
    val data = spark.read.parquet(path)
    data.show(truncate = false)
  }

}
