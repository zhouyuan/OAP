package com.intel.sparkColumnarPlugin.vectorized;

/** Wrapper for ArrowFieldNodeBuilder. */
public class ArrowFieldNodeBuilder {

  public int length;
  public int nullCount;

  /**
   * Create an instance of ArrowFieldNodeBuilder.
   *
   * @param length numRows in this Field
   * @param nullCount numCount in this Fields
   */
  public ArrowFieldNodeBuilder(int length, int nullCount) {
    this.length = length;
    this.nullCount = nullCount;
  }
}
