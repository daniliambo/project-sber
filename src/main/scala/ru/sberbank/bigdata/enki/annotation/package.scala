package ru.sberbank.bigdata.enki

import scala.reflect.runtime.currentMirror
import scala.reflect.runtime.universe._
import scala.tools.reflect.ToolBox

/** Generic annotations support. */
package object annotation {
  private lazy val toolbox = currentMirror.mkToolBox()

  def instantiate(annotation: Annotation): Any = toolbox.eval(toolbox.untypecheck(annotation.tree))
}
