package ru.sberbank.bigdata.enki.plan.printer

import org.apache.spark.sql.catalyst.expressions._

import scala.annotation.tailrec
import scala.collection.immutable.Queue
import scala.reflect.ClassTag

object expressions {

  def flattenBinaryOperator[T <: BinaryOperator: ClassTag](op: T): List[Expression] = {
    @tailrec
    def go(acc: Queue[Expression], chain: List[Expression]): List[Expression] =
      if (acc.isEmpty) {
        chain.reverse
      } else {
        val (head, tail) = acc.dequeue
        head match {
          case op: T => go(tail.enqueue(op.children.toList), chain)
          case other => go(tail, other :: chain)
        }
      }

    go(Queue(op), Nil)
  }

  final case class NotWrapper(child: Expression) extends UnaryExpression with Predicate with Unevaluable {

    override def sql: String = child match {
      case _: AndWrapper | _: OrWrapper => "NOT (" + child.sql + ")"
      case other                        => "NOT " + other.sql
    }
  }

  final case class AndWrapper(children: Seq[Expression]) extends Predicate with Unevaluable {
    override def nullable: Boolean = children.exists(_.nullable)

    override def sql: String = children.map {
      case or: OrWrapper => "(" + or.sql + ")"
      case other         => other.sql
    }.mkString(" AND ")
  }

  final case class OrWrapper(children: Seq[Expression]) extends Predicate with Unevaluable {
    override def nullable: Boolean = children.exists(_.nullable)

    override def sql: String = children.map(_.sql).mkString(" OR ")
  }

}
