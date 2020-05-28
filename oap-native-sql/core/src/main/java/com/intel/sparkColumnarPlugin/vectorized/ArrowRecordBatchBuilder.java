package com.intel.sparkColumnarPlugin.vectorized;

/** ArrowRecordBatchBuilder. */
public class ArrowRecordBatchBuilder {

  public int length;
  public ArrowFieldNodeBuilder[] nodeBuilders;
  public ArrowBufBuilder[] bufferBuilders;

  /**
   * Create an instance to wrap native parameters for ArrowRecordBatchBuilder.
   *
   * @param length ArrowRecordBatch rowNums.
   * @param nodeBuilders an Array hold ArrowFieldNodeBuilder.
   * @param bufferBuilders an Array hold ArrowBufBuilder.
   */
  public ArrowRecordBatchBuilder(
      int length, ArrowFieldNodeBuilder[] nodeBuilders, ArrowBufBuilder[] bufferBuilders) {
    this.length = length;
    this.nodeBuilders = nodeBuilders;
    this.bufferBuilders = bufferBuilders;
  }
}
