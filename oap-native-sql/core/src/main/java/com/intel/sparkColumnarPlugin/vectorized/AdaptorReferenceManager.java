package com.intel.sparkColumnarPlugin.vectorized;

import java.io.IOException;
import java.lang.UnsupportedOperationException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OwnershipTransferResult;
import org.apache.arrow.memory.ReferenceManager;
import org.apache.arrow.util.Preconditions;

import io.netty.buffer.ArrowBuf;

/**
 * A simple reference manager implementation for memory allocated by native code. The underlying
 * memory will be released when reference count reach zero.
 */
public class AdaptorReferenceManager implements ReferenceManager {
  private native void nativeRelease(long nativeMemoryHolder);

  private final AtomicInteger bufRefCnt = new AtomicInteger(0);
  private long nativeMemoryHolder;
  private int size = 0;

  AdaptorReferenceManager(long nativeMemoryHolder, int size) throws IOException {
    JniUtils.getInstance();
    this.nativeMemoryHolder = nativeMemoryHolder;
    this.size = size;
  }

  @Override
  public int getRefCount() {
    return bufRefCnt.get();
  }

  @Override
  public boolean release() {
    return release(1);
  }

  @Override
  public boolean release(int decrement) {
    Preconditions.checkState(
        decrement >= 1, "ref count decrement should be greater than or equal to 1");
    // decrement the ref count
    final int refCnt;
    synchronized (this) {
      refCnt = bufRefCnt.addAndGet(-decrement);
      if (refCnt == 0) {
        // refcount of this reference manager has dropped to 0
        // release the underlying memory
        nativeRelease(nativeMemoryHolder);
      }
    }
    // the new ref count should be >= 0
    Preconditions.checkState(refCnt >= 0, "RefCnt has gone negative");
    return refCnt == 0;
  }

  @Override
  public void retain() {
    retain(1);
  }

  @Override
  public void retain(int increment) {
    Preconditions.checkArgument(increment > 0, "retain(%d) argument is not positive", increment);
    bufRefCnt.addAndGet(increment);
  }

  @Override
  public ArrowBuf retain(ArrowBuf srcBuffer, BufferAllocator targetAllocator) {
    retain();
    return srcBuffer;
  }

  @Override
  public ArrowBuf deriveBuffer(ArrowBuf sourceBuffer, int index, int length) {
    final long derivedBufferAddress = sourceBuffer.memoryAddress() + index;

    // create new ArrowBuf
    final ArrowBuf derivedBuf = new ArrowBuf(this, null, length, derivedBufferAddress, false);

    return derivedBuf;
  }

  @Override
  public OwnershipTransferResult transferOwnership(
      ArrowBuf sourceBuffer, BufferAllocator targetAllocator) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BufferAllocator getAllocator() {
    return null;
  }

  @Override
  public int getSize() {
    return size;
  }

  @Override
  public int getAccountedSize() {
    return 0;
  }
}
