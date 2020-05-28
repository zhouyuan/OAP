package com.intel.sparkColumnarPlugin.vectorized;

/** ArrowBufBuilder. */
public class ArrowBufBuilder {

  public long nativeInstanceId;
  public long memoryAddress;
  public int size;
  public long capacity;

  /**
   * Create an instance for ArrowBufBuilder.
   *
   * @param nativeInstanceId native ArrowBuf holder.
   * @param memoryAddress native ArrowBuf data addr.
   * @param size ArrowBuf size.
   * @param capacity ArrowBuf rowNums.
   */
  public ArrowBufBuilder(long nativeInstanceId, long memoryAddress, int size, long capacity) {
    this.memoryAddress = memoryAddress;
    this.size = size;
    this.capacity = capacity;
    this.nativeInstanceId = nativeInstanceId;
  }
}
