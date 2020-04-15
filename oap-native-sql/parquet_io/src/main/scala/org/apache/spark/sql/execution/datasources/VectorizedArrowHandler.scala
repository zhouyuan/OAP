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

package org.apache.spark.sql.execution.datasources

import java.lang.Long
import java.time.ZoneId

import scala.collection.mutable.Map

import org.apache.arrow.adapter.parquet._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.memory.RootAllocator
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.parquet.hadoop.ParquetInputSplit

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class VectorizedArrowReaderHandler() extends Logging {

  val allocator = new RootAllocator(Long.MAX_VALUE)
  def getAllocator(): BufferAllocator = allocator

  def getParquetReader(
    split: ParquetInputSplit,
    convertTz: ZoneId,
    useOffHeap: Boolean,
    capacity: Int,
    sourceSchema: StructType,
    readDataSchema: StructType
  ): VectorizedParquetArrowReader = synchronized {
    val filePath = split.getPath().toString()
    new VectorizedParquetArrowReader(
      allocator, filePath,
      convertTz, useOffHeap, capacity, sourceSchema, readDataSchema)
  }

  def close(): Unit = synchronized {
    allocator.close();
  }
}

class VectorizedArrowWriterHandler() extends Logging {

  @throws(classOf[Exception])
  def getParquetWriter (
    taskAttemptContext: TaskAttemptContext,
    path: String
  ): VectorizedParquetArrowWriter = synchronized {
    new VectorizedParquetArrowWriter(taskAttemptContext, path)
  }

  def close(): Unit = synchronized {
  }
}

object VectorizedArrowHandler {

  var readerHandler: VectorizedArrowReaderHandler = _
  var writerHandler: VectorizedArrowWriterHandler = _

  def getReader(): VectorizedArrowReaderHandler = synchronized {
    if (readerHandler == null) {
      readerHandler = new VectorizedArrowReaderHandler()
    }
    readerHandler
  }

  def getWriter(): VectorizedArrowWriterHandler = synchronized {
    if (writerHandler == null) {
      writerHandler = new VectorizedArrowWriterHandler()
    }
    writerHandler
  }

  def close(): Unit = {
    if (readerHandler != null) {
      readerHandler.close()
    }
    if (writerHandler != null) {
      writerHandler.close()
    }
  }

}
