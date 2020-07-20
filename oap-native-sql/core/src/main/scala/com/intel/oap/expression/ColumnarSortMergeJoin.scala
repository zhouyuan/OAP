/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.oap.expression

import java.util.concurrent.TimeUnit._

import com.intel.oap.ColumnarPluginConfig
import com.intel.oap.vectorized.ArrowWritableColumnVector

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}

import scala.collection.JavaConverters._
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import scala.collection.mutable.ListBuffer
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.gandiva.evaluator._

import io.netty.buffer.ArrowBuf
import com.google.common.collect.Lists;

import org.apache.spark.sql.types.{DataType, StructType}
import com.intel.oap.vectorized.ExpressionEvaluator
import com.intel.oap.vectorized.BatchIterator

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
class ColumnarSortMergeJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    resultSchema: StructType,
    joinType: JoinType,
    conditionOption: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean,
    joinTime: SQLMetric,
    totalOutputNumRows: SQLMetric,
    sparkConf: SparkConf)
    extends Logging {
  ColumnarPluginConfig.getConf(sparkConf)

  def close(): Unit = {
  }
}

object ColumnarSortMergeJoin {
  var columnarSortMergeJoin: ColumnarSortMergeJoin = _
  def create(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      resultSchema: StructType,
      joinType: JoinType,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean,
      joinTime: SQLMetric,
      numOutputRows: SQLMetric,
      sparkConf: SparkConf): ColumnarSortMergeJoin = synchronized {
    columnarSortMergeJoin = new ColumnarSortMergeJoin(
      leftKeys,
      rightKeys,
      resultSchema,
      joinType,
      condition,
      left,
      right,
      isSkewJoin,
      joinTime,
      numOutputRows,
      sparkConf)
    columnarSortMergeJoin
  }

  def close(): Unit = {
    if (columnarSortMergeJoin != null) {
      columnarSortMergeJoin.close()
    }
  }
}
