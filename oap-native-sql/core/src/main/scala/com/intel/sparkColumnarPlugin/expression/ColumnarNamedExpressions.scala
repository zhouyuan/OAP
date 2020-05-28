package com.intel.sparkColumnarPlugin.expression

import org.apache.arrow.gandiva.evaluator._
import org.apache.arrow.gandiva.exceptions.GandivaException
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

class ColumnarAlias(child: Expression, name: String)(
    override val exprId: ExprId,
    override val qualifier: Seq[String],
    override val explicitMetadata: Option[Metadata])
    extends Alias(child, name)(exprId, qualifier, explicitMetadata)
    with ColumnarExpression {

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    child.asInstanceOf[ColumnarExpression].doColumnarCodeGen(args)
  }

}

class ColumnarAttributeReference(
    name: String,
    dataType: DataType,
    nullable: Boolean = true,
    override val metadata: Metadata = Metadata.empty)(
    override val exprId: ExprId,
    override val qualifier: Seq[String])
    extends AttributeReference(name, dataType, nullable, metadata)(exprId, qualifier)
    with ColumnarExpression {

  override def doColumnarCodeGen(args: java.lang.Object): (TreeNode, ArrowType) = {
    val resultType = CodeGeneration.getResultType(dataType)
    val field = Field.nullable(s"${name}#${exprId.id}", resultType)
    (TreeBuilder.makeField(field), resultType)
  }

}
