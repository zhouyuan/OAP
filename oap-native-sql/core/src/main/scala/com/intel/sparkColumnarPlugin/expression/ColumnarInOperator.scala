package com.intel.sparkColumnarPlugin.expression

import com.google.common.collect.Lists
import com.google.common.collect.Sets

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

/**
 * A version of substring that supports columnar processing for utf8.
 */
class ColumnarIn(value: Expression, list: Seq[Expression], original: Expression)
    extends In(value: Expression, list: Seq[Expression])
    with ColumnarExpression
    with Logging {
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val (value_node, valueType): (TreeNode, ArrowType) =
      value.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)

    val resultType = new ArrowType.Bool()

    if (value.dataType == StringType) {
      logInfo(s"make string in")
      val newlist :List[String]= list.toList.map (expr => {
        expr.asInstanceOf[Literal].value.toString
      });
      val tlist = Lists.newArrayList(newlist:_*);

      val funcNode = TreeBuilder.makeInExpressionString(value_node, Sets.newHashSet(tlist))
      (funcNode, resultType)
    } else if (value.dataType == IntegerType) {
      val newlist :List[Integer]= list.toList.map (expr => {
        expr.asInstanceOf[Literal].value.asInstanceOf[Integer]
      });
      val tlist = Lists.newArrayList(newlist:_*);

      val funcNode = TreeBuilder.makeInExpressionInt32(value_node, Sets.newHashSet(tlist))
      (funcNode, resultType)
    } else if (value.dataType == LongType) {
      val newlist :List[java.lang.Long]= list.toList.map (expr => {
        expr.asInstanceOf[Literal].value.asInstanceOf[java.lang.Long]
      });
      val tlist = Lists.newArrayList(newlist:_*);

      val funcNode = TreeBuilder.makeInExpressionBigInt(value_node, Sets.newHashSet(tlist))
      (funcNode, resultType)
    } else {
      throw new UnsupportedOperationException(s"not currently supported: ${value.dataType}.")
    }
  }
}

object ColumnarInOperator {

  def create(value: Expression, list: Seq[Expression], original: Expression): Expression = original match {
    case i: In =>
      new ColumnarIn(value, list, i)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
