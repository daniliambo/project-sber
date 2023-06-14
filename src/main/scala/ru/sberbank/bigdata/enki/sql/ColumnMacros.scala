package ru.sberbank.bigdata.enki.sql

import org.apache.spark.sql.Encoder

import scala.collection.immutable.Queue
import scala.language.experimental.macros
import scala.reflect.macros._

// shamelessly taken from frameless
class ColumnMacros(val c: whitebox.Context) {

  import c.universe._

  def fromFunction[A: WeakTypeTag, B: WeakTypeTag, R: WeakTypeTag](
    selector: c.Expr[A => B]
  )(relation: c.Expr[TypeRelation[A, R]], encoder: c.Expr[Encoder[R]]): Tree = {
    def fail(tree: Tree) = {
      val err =
        s"Could not create a column identifier from $tree - try using _.a.b syntax"
      c.abort(tree.pos, err)
    }

    val A = weakTypeOf[A].dealias
    val R = weakTypeOf[R].dealias

    val selectorStr = selector.tree match {
      case NameExtractor(str) => str
      case Function(_, body)  => fail(body)
      // $COVERAGE-OFF$ - cannot be reached as typechecking will fail in this case before macro is even invoked
      case other => fail(other)
      // $COVERAGE-ON$
    }

    val datasetCol = c.typecheck(q"${c.prefix}.col[$R]($selectorStr)($encoder)")

    c.typecheck(datasetCol)
  }

  private class NameExtractor(name: TermName) {
    Self =>

    def unapply(tree: Tree): Option[Queue[String]] =
      tree match {
        case Ident(`name`)              => Some(Queue.empty)
        case Select(Self(strs), nested) => Some(strs.enqueue(nested.toString))
        // $COVERAGE-OFF$ - Not sure if this case can ever come up and Encoder will still work
        case Apply(Self(strs), List()) => Some(strs)
        // $COVERAGE-ON$
        case _ => None
      }
  }

  private object NameExtractor {

    def unapply(tree: Tree): Option[String] = tree match {
      case Function(List(ValDef(_, name, argTyp, _)), body) => NameExtractor(name).unapply(body).map(_.mkString("."))
      case _                                                => None
    }

    def apply(name: TermName): NameExtractor = new NameExtractor(name)
  }

}
