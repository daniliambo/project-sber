package generatedata.test

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.DataType

class ExpressionGenerator extends  LeafExpression with CodegenFallback {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = ???

  override def dataType: DataType = ???

  override def productElement(n: Int): Any = ???

  override def productArity: Int = ???

  override def canEqual(that: Any): Boolean = ???
}
