package ru.sberbank.bigdata.enki.plan.printer

object util {

  def appendToBuilder(str: String, builder: StringBuilder, depth: Int, newline: Boolean = true): Unit = {
    if (depth > 0) builder ++= (" " * depth)

    builder ++= str

    if (newline) builder += '\n'
  }

}
