package com.intel.sparkColumnarPlugin.expression

import io.netty.buffer.ArrowBuf
import java.util._
import java.util.concurrent.TimeUnit

import com.google.common.collect.Lists
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.sql.execution.vectorized.ArrowWritableColumnVector

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.ValueVector

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.List
import scala.collection.mutable.ArrayBuffer

class ColumnarProjection (
  originalInputAttributes: Seq[Attribute],
  exprs: Seq[Expression],
  skipLiteral: Boolean = false,
  renameResult: Boolean = false) extends AutoCloseable with Logging {
  // build gandiva projection here.
  //////////////// Project original input to aggregate input //////////////////
  var projector : Projector = null
  var inputList : java.util.List[Field] = Lists.newArrayList()
  val expressionList = if (skipLiteral) {
    exprs.filter(expr => !expr.isInstanceOf[Literal])
  } else {
    exprs
  }
  val resultAttributes = expressionList.toList.zipWithIndex.map{case (expr, i) =>
    if (renameResult) {
      ConverterUtils.getResultAttrFromExpr(expr, s"res_$i")
    } else {
      ConverterUtils.getResultAttrFromExpr(expr)
    }
  }
  var check_if_no_calculation = true
  val projPrepareList : Seq[ExpressionTree] = expressionList.zipWithIndex.map {
    case (expr, i) => {
      ColumnarExpressionConverter.reset()
      var columnarExpr: Expression =
        ColumnarExpressionConverter.replaceWithColumnarExpression(expr, originalInputAttributes)
      if (columnarExpr.isInstanceOf[AttributeReference]) {
        columnarExpr = new ColumnarBoundReference(i, columnarExpr.dataType, columnarExpr.nullable)
      }
      if (ColumnarExpressionConverter.ifNoCalculation == false) {
        check_if_no_calculation = false     
      }
      logInfo(s"columnarExpr is ${columnarExpr}")
      val (node, resultType) =
        columnarExpr.asInstanceOf[ColumnarExpression].doColumnarCodeGen(inputList)
      TreeBuilder.makeExpression(node, Field.nullable(s"res_$i", resultType))
    }
  }

  val (ordinalList, arrowSchema) = if (projPrepareList.size > 0 && check_if_no_calculation == false) {
    val inputFieldList = inputList.asScala.toList.distinct
    val schema = new Schema(inputFieldList.asJava)
    projector = Projector.make(schema, projPrepareList.toList.asJava)
    (inputFieldList.map(field => {
      field.getName.replace("c_", "").toInt
    }),
    schema)
  } else {
    val inputFieldList = inputList.asScala.toList
    (inputFieldList.map(field => {
      field.getName.replace("c_", "").toInt
    }),
    new Schema(inputFieldList.asJava))
  }
  logInfo(s"Project input ordinal is ${ordinalList}, Schema is ${arrowSchema}")
  val outputArrowSchema = new Schema(resultAttributes.map(attr => {
    Field.nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
  }).asJava)
  val outputSchema = ArrowUtils.fromArrowSchema(outputArrowSchema)

  def output(): List[AttributeReference] = {
    resultAttributes
  }

  def getOrdinalList(): List[Int] = {
    ordinalList 
  }

  def needEvaluate : Boolean = { projector != null }
  def evaluate(numRows: Int, inputColumnVector: List[ValueVector]): List[ArrowWritableColumnVector] = {
    if (projector != null) {
      val inputRecordBatch: ArrowRecordBatch = ConverterUtils.createArrowRecordBatch(numRows, inputColumnVector)
      val outputVectors = ArrowWritableColumnVector.allocateColumns(numRows, outputSchema)
      val valueVectors = outputVectors.map(columnVector => columnVector.getValueVector()).toList
      projector.evaluate(inputRecordBatch, valueVectors.asJava)
      ConverterUtils.releaseArrowRecordBatch(inputRecordBatch)
      outputVectors.toList
    } else {
      List[ArrowWritableColumnVector]()
    }
  }

  override def close(): Unit = {
    if (projector != null) {
      projector.close()
      projector = null
    }
  }
}

object ColumnarProjection extends Logging {
  def create(
    originalInputAttributes: Seq[Attribute],
    exprs: Seq[Expression],
    skipLiteral: Boolean = false,
    renameResult: Boolean = false)
    : ColumnarProjection = {
    new ColumnarProjection(originalInputAttributes, exprs, skipLiteral, renameResult)
  }
}
