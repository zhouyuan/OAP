package com.intel.sparkColumnarPlugin.expression

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.DateUnit

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class ColumnarLiteral(lit: Literal)
    extends Literal(lit.value, lit.dataType)
    with ColumnarExpression {

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val resultType = CodeGeneration.getResultType(dataType)
    dataType match {
      case t: StringType =>
        (TreeBuilder.makeStringLiteral(value.toString().asInstanceOf[String]), resultType)
      case t: IntegerType =>
        (TreeBuilder.makeLiteral(value.asInstanceOf[Integer]), resultType)
      case t: LongType =>
        (TreeBuilder.makeLiteral(value.asInstanceOf[java.lang.Long]), resultType)
      case t: DoubleType =>
        (TreeBuilder.makeLiteral(value.asInstanceOf[java.lang.Double]), resultType)
      case d: DecimalType =>
        val v = value.asInstanceOf[Decimal]
        (TreeBuilder.makeDecimalLiteral(v.toString, v.precision, v.scale), resultType)
      case d: DateType =>
        val stringDate = DateTimeUtils.toJavaDate(value.asInstanceOf[Integer])
        (TreeBuilder.makeStringLiteral(stringDate.toString), new ArrowType.Utf8())
        //throw new UnsupportedOperationException(s"DateType is not supported yet.")
    }
  }
}
