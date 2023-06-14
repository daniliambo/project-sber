package generatedata.modules

import generatedata.modules.module1.struct.{TableScheme1, TableScheme2, TableScheme3}
import ru.sberbank.bigdata.enki.framework.SourceTable

object Tables {

  val getter1: SourceTable[TableScheme1] = new SourceTable[TableScheme1] {
    override def schema: String    = "scheme1"
    override def name: String      = "table1"
    override def className: String = "t1"
  }

  val getter2: SourceTable[TableScheme2] = new SourceTable[TableScheme2] {
    override def schema: String    = "scheme2"
    override def name: String      = "table2"
    override def className: String = "t2"
  }

  val getter3: SourceTable[TableScheme3] = new SourceTable[TableScheme3] {
    override def schema: String    = "scheme3"
    override def name: String      = "table3"
    override def className: String = "t3"
  }
}
