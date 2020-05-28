package com.intel.sparkColumnarPlugin.vectorized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import io.netty.buffer.ArrowBuf;

/** ArrowRecordBatchBuilderImpl used to wrap native returned data into an ArrowRecordBatch. */
public class ArrowRecordBatchBuilderImpl {

  private int length;
  private ArrowRecordBatchBuilder recordBatchBuilder;

  /**
   * Create ArrowRecordBatchBuilderImpl instance from ArrowRecordBatchBuilder.
   *
   * @param recordBatchBuilder ArrowRecordBatchBuilder instance.
   */
  public ArrowRecordBatchBuilderImpl(ArrowRecordBatchBuilder recordBatchBuilder) {
    this.recordBatchBuilder = recordBatchBuilder;
  }

  /**
   * Build ArrowRecordBatch from ArrowRecordBatchBuilder instance.
   *
   * @throws IOException throws exception
   */
  public ArrowRecordBatch build() throws IOException {
    if (recordBatchBuilder.length == 0) {
      return null;
    }

    List<ArrowFieldNode> nodes = new ArrayList<ArrowFieldNode>();
    for (ArrowFieldNodeBuilder tmp : recordBatchBuilder.nodeBuilders) {
      nodes.add(new ArrowFieldNode(tmp.length, tmp.nullCount));
    }

    List<ArrowBuf> buffers = new ArrayList<ArrowBuf>();
    for (ArrowBufBuilder tmp : recordBatchBuilder.bufferBuilders) {
      AdaptorReferenceManager referenceManager =
          new AdaptorReferenceManager(tmp.nativeInstanceId, tmp.size);
      buffers.add(new ArrowBuf(referenceManager, null, tmp.size, tmp.memoryAddress, false));
    }
    return new ArrowRecordBatch(recordBatchBuilder.length, nodes, buffers);
  }
}
