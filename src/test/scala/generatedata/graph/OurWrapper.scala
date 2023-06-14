package generatedata.graph

import org.apache.spark.sql.catalyst.expressions.{BinaryComparison, Expression, NullIntolerant}
import org.apache.spark.sql.types.AbstractDataType

final case class OurWrapper(delegate: BinaryComparison) extends BinaryComparison with NullIntolerant {

  override def inputType = delegate.inputType

  override def symbol: String = delegate.symbol

  override def left: Expression = delegate.left

  override def right: Expression = delegate.right

  override def nullSafeEval(input1: Any, input2: Any): Any = super.nullSafeEval(input1, input2)
}
