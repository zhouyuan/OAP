package com.intel.sparkColumnarPlugin.expression

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.execution.BaseSubqueryExec
import org.apache.spark.sql.execution.ExecSubqueryExpression
import org.apache.spark.sql.execution.ScalarSubquery
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class ColumnarScalarSubquery(
  query: ScalarSubquery)
  extends Expression with ColumnarExpression {

  override def dataType: DataType = query.dataType
  override def children: Seq[Expression] = Nil
  override def nullable: Boolean = true
  override def toString: String = query.toString
  override def eval(input: InternalRow): Any = query.eval(input)
  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = query.doGenCode(ctx, ev)
  override def canEqual(that: Any): Boolean = query.canEqual(that)
  override def productArity: Int = query.productArity
  override def productElement(n: Int): Any = query.productElement(n)
  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val value = query.eval(null)
    val resultType = CodeGeneration.getResultType(query.dataType)
    query.dataType match {
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
        throw new UnsupportedOperationException(s"DateType is not supported yet.")
    }
  }
}
