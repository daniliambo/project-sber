package ru.sberbank.bigdata.enki.framework

trait TableNameFromClass extends TableName {

  override def name: String = {
    val letters = this.getClass.getSimpleName.takeWhile(_ != '$')
    if (letters.length == 0)
      throw new Exception("Unable to infer table name.")
    val name = letters.head.toLower +: letters.tail.flatMap { c =>
      if (c.isUpper) s"_${c.toLower}" else c.toString
    }
    if (name.length > 5 && name.endsWith("_view"))
      name.substring(0, name.length - 5)
    else
      name
  }
}
