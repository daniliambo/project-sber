package ru.sberbank.bigdata.enki.framework

import java.io.{PrintStream, PrintWriter}

class AggregateException(
  val message: String,
  val causes: Seq[Throwable]
) extends Exception(message, causes.headOption.orNull) {

  override def printStackTrace(err: PrintStream): Unit = {
    super.printStackTrace(err)
    causes.zipWithIndex.foreach { a: (Throwable, Int) =>
      err.append("\n")
      err.append(s"  Inner throwable #${a._2}")
      err.append(": ")
      a._1.printStackTrace(err)
      err.append("\n")
    }
  }

  override def printStackTrace(err: PrintWriter): Unit = {
    super.printStackTrace(err)
    causes.zipWithIndex.foreach { a: (Throwable, Int) =>
      err.append("\n")
      err.append(s"  Inner throwable #${a._2}")
      err.append(": ")
      a._1.printStackTrace(err)
      err.append("\n")
    }
  }
}
