package com.intel.sparkColumnarPlugin.expression

import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils

object CodeGeneration {
  val timeZoneId = SQLConf.get.sessionLocalTimeZone

  def getResultType(left: ArrowType, right: ArrowType): ArrowType = {
    if (left.equals(right)) {
      left
    } else {
      val left_precise_level = getPreciseLevel(left)
      val right_precise_level = getPreciseLevel(right)
      if (left_precise_level > right_precise_level)
        left
      else
        right
    }
  }

  def getResultType(dataType: DataType): ArrowType = {
    dataType match {
      case other =>
        ArrowUtils.toArrowType(dataType, timeZoneId)
    }
    /*dataType match {
    case t: IntegerType =>
      new ArrowType.Int(32, true)
    case l: LongType =>
      new ArrowType.Int(64, true)
    case t: FloatType =>
      new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case t: DoubleType =>
      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case d: DecimalType =>
      new ArrowType.Decimal(d.precision, d.scale)
    case s: StringType =>
      new ArrowType.Utf8()
    case other =>
      throw new UnsupportedOperationException(s"getResultType doesn't support $other.")
      */
  }

  def getResultType(): ArrowType = {
    new ArrowType.Int(32, true)
  }

  def getPreciseLevel(dataType: ArrowType): Int = {
    dataType match {
      case t: ArrowType.Int =>
        4
      case t: ArrowType.FloatingPoint =>
        8
      case _ =>
        throw new UnsupportedOperationException(s"Unable to get precise level of $dataType ${dataType.getClass}.")
    }
  }

  def getCastFuncName(dataType: ArrowType): String = {
    dataType match {
      case t: ArrowType.FloatingPoint =>
        s"castFLOAT${4 * dataType.asInstanceOf[ArrowType.FloatingPoint].getPrecision().getFlatbufID()}"
      case _ =>
        throw new UnsupportedOperationException(s"getCastFuncName(${dataType}) is not supported.")
    }
  }
}
