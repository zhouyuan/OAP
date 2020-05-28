package com.intel.sparkColumnarPlugin.expression;

import java.io.*;
import io.netty.buffer.ArrowBuf;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.ValueVector;

public class ArrowColumnarBatch implements AutoCloseable {

  int fieldsNum;
  int numRowsInBatch;
  List<ValueVector> valueVectors = null;

  ArrowColumnarBatch(List<ValueVector> valueVectors, int fieldsNum, int numRowsInBatch) {
    this.valueVectors = valueVectors;
    this.fieldsNum = fieldsNum;
    this.numRowsInBatch = numRowsInBatch;
  }

  @Override
  public void close() throws IOException {}
}
