package com.intel.sparkColumnarPlugin.expression

import com.google.common.collect.Lists
import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import scala.collection.mutable.ListBuffer

class ColumnarBoundReference(ordinal: Int, dataType: DataType, nullable: Boolean)
    extends BoundReference(ordinal, dataType, nullable)
    with ColumnarExpression with Logging {

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val resultType = CodeGeneration.getResultType(dataType)
    val field = Field.nullable(s"c_$ordinal", resultType)
    val fieldTypes = args.asInstanceOf[java.util.List[Field]]
    fieldTypes.add(field)
    (TreeBuilder.makeField(field), resultType)
  }

}
