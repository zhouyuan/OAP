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
package org.apache.spark.sql.execution.datasources.v2.arrow

import org.apache.arrow.dataset.Dataset
import org.apache.arrow.dataset.jni.{NativeDataSource, NativeScanner}
import org.apache.arrow.dataset.scanner.ScanOptions
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.execution.vectorized.{ArrowWritableColumnVector, ColumnVectorUtils, OnHeapColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.reader.{InputPartition, PartitionReader}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._

case class ArrowPartitionReaderFactory(
    sqlConf: SQLConf,
    broadcastedConf: Broadcast[SerializableConfiguration],
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    pushedFilters: Array[Filter],
    options: ArrowOptions)
    extends FilePartitionReaderFactory {

  private val batchSize = 4096
  private val enableFilterPushDown: Boolean = sqlConf
    .getConfString("spark.sql.arrow.filterPushdown", "true").toBoolean

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    // disable row based read
    throw new UnsupportedOperationException
  }

  def loadVsr(vsr: VectorSchemaRoot, partitionValues: InternalRow): ColumnarBatch = {
    val fvs = vsr.getFieldVectors

    val rowCount = vsr.getRowCount
    val vectors = ArrowWritableColumnVector.loadColumns(rowCount, fvs)
    val partitionColumns = OnHeapColumnVector.allocateColumns(rowCount, readPartitionSchema)
    (0 until partitionColumns.length).foreach(i => {
      ColumnVectorUtils.populate(partitionColumns(i), partitionValues, i)
      partitionColumns(i).setIsConstant()
    })

    val batch = new ColumnarBatch(vectors ++ partitionColumns, rowCount, new Array[Long](5))
    batch.setNumRows(rowCount)
    batch
  }

  override def buildColumnarReader(
      partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val path = partitionedFile.filePath
    val discovery = ArrowUtils.makeArrowDiscovery(path, options)
    val source = discovery.finish()

    val dataset = new Dataset[NativeDataSource](List(source).asJava,
      discovery.inspect())
    val filter = if (enableFilterPushDown) {
      ArrowFilters.translateFilters(pushedFilters)
    } else {
      org.apache.arrow.dataset.filter.Filter.EMPTY
    }
    val scanner = new NativeScanner(dataset,
      // todo predicate validation / pushdown
      new ScanOptions(readDataSchema.map(f => f.name).toArray,
        filter, batchSize),
      org.apache.spark.sql.util.ArrowUtils.rootAllocator)
    val itrList = scanner
      .scan()
      .iterator()
      .asScala
      .map(task => task.scan())
      .toList

    val itr = itrList
      .toIterator
      .flatMap(itr => itr.asScala)
      .map(vsr => loadVsr(vsr, partitionedFile.partitionValues))

    new PartitionReader[ColumnarBatch] {

      override def next(): Boolean = {
        itr.hasNext
      }

      override def get(): ColumnarBatch = {
        itr.next()
      }

      override def close(): Unit = {
        itrList.foreach(itr => itr.close())
        scanner.close() // todo memory leak?
      }
    }
  }
}