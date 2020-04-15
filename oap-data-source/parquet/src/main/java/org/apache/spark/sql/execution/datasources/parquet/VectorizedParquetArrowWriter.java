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

package org.apache.spark.sql.execution.datasources.parquet;

import static org.apache.parquet.Preconditions.checkNotNull;

import io.netty.buffer.ArrowBuf;
import java.io.*;
import java.util.*;

import org.apache.arrow.adapter.parquet.*;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.arrow.schema.SchemaMapping;

import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.execution.vectorized.ArrowWritableColumnVector;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizedParquetArrowWriter extends RecordWriter<Void, ColumnarBatch> {

  private static final Logger LOG = LoggerFactory.getLogger(VectorizedParquetArrowWriter.class);
  private String path;
  private WriteSupport<ColumnarBatch> writeSupport;
  private List<ArrowRecordBatch> recordBatchList = new ArrayList<ArrowRecordBatch>();

  public ParquetWriter writer;
  public static final String WRITE_SUPPORT_CLASS  = "parquet.write.support.class";
  private Configuration configuration;

  public VectorizedParquetArrowWriter(
      TaskAttemptContext taskAttemptContext,
      String path) throws IOException {
    this.path = path;
    configuration = taskAttemptContext.getConfiguration();
    WriteSupport<ColumnarBatch> writeSupport = getWriteSupport(configuration);
    WriteContext init = writeSupport.init(configuration);

    MessageType schema = init.getSchema();
    SchemaConverter converter = new SchemaConverter();
    Schema arrowSchema = converter.fromParquet(schema).getArrowSchema();

    String uriPath = this.path;
    if (uriPath.contains("hdfs")) {
      uriPath = this.path + "?user=root&replication=1&use_hdfs3=1";
    }
    LOG.info("ParquetReader uri path is " + uriPath);
    writer = new ParquetWriter(uriPath, arrowSchema);
  }

  @Override
  public void write(Void key, ColumnarBatch value) throws IOException, InterruptedException {
    ArrowRecordBatch inputRecordBatch = createArrowRecordBatch(value);
    writer.writeNext(inputRecordBatch);
    recordBatchList.add(inputRecordBatch);
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    writer.close();
    for (ArrowRecordBatch inputRecordBatch : recordBatchList) {
      releaseArrowRecordBatch(inputRecordBatch);
    }
  }

  private ArrowRecordBatch createArrowRecordBatch(ColumnarBatch columnarBatch) {
    List<ArrowFieldNode> fieldNodes = new ArrayList<ArrowFieldNode>();
    List<ArrowBuf> inputData = new ArrayList<ArrowBuf>();
    int numRowsInBatch = columnarBatch.numRows();
    for (int i = 0; i < columnarBatch.numCols(); i++) {
      ValueVector inputVector = ((ArrowWritableColumnVector)columnarBatch.column(i)).getValueVector();
      fieldNodes.add(new ArrowFieldNode(numRowsInBatch, inputVector.getNullCount()));
      inputData.add(inputVector.getValidityBuffer());
      inputData.add(inputVector.getDataBuffer());
    }
    return new ArrowRecordBatch(numRowsInBatch, fieldNodes, inputData);
  }

  private void releaseArrowRecordBatch(ArrowRecordBatch recordBatch) {
    recordBatch.close();
  }

  /**
   * @param configuration to find the configuration for the write support class
   * @return the configured write support
   */
  @SuppressWarnings("unchecked")
  public WriteSupport<ColumnarBatch> getWriteSupport(Configuration configuration){
    if (writeSupport != null) return writeSupport;
    Class<?> writeSupportClass = getWriteSupportClass(configuration);
    try {
      return (WriteSupport<ColumnarBatch>)checkNotNull(writeSupportClass, "writeSupportClass").newInstance();
    } catch (InstantiationException e) {
      throw new BadConfigurationException("could not instantiate write support class: " + writeSupportClass, e);
    } catch (IllegalAccessException e) {
      throw new BadConfigurationException("could not instantiate write support class: " + writeSupportClass, e);
    }
  }

  public static Class<?> getWriteSupportClass(Configuration configuration) {
    final String className = configuration.get(WRITE_SUPPORT_CLASS);
    if (className == null) {
      return null;
    }
    final Class<?> writeSupportClass = ConfigurationUtil.getClassFromConfig(configuration, WRITE_SUPPORT_CLASS, WriteSupport.class);
    return writeSupportClass;
  }

}
