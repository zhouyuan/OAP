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

package com.intel.oap.vectorized;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

/** ArrowBufBuilder. */
public class SerializableObject implements Serializable {
  public int total_size;
  public int[] size;
  public byte[] data;
  private transient ByteBuf[] directAddrs;
  private transient ByteBufAllocator allocator;

  /**
   * Create an instance for NativeSerializableObject.
   *
   * @param memoryAddress native ArrowBuf data addr.
   * @param size ArrowBuf size.
   */
  public SerializableObject(long[] memoryAddress, int[] size) throws IOException {
    this.total_size = 0;
    this.size = size;
    ByteBuf[] bufList = new ByteBuf[size.length];
    for (int i = 0; i < size.length; i++) {
      this.total_size += size[i];
      bufList[i] = Unpooled.wrappedBuffer(memoryAddress[i], size[i], false);
    }

    this.data = new byte[this.total_size];
    int off = 0;
    for (int i = 0; i < size.length; i++) {
      ByteBufInputStream in = new ByteBufInputStream(bufList[i]);
      in.read(data, off, size[i]);
      off += size[i];
    }
  }

  /**
   * Create an instance for NativeSerializableObject.
   *
   * @param memoryAddress native ArrowBuf data addr.
   * @param size ArrowBuf size.
   */
  public SerializableObject(NativeSerializableObject obj) throws IOException {
    this(obj.memoryAddress, obj.size);
  }

  public long[] getDirectMemoryAddrs() throws IOException {
    if (directAddrs == null) {
      allocator = UnpooledByteBufAllocator.DEFAULT;
      directAddrs = new ByteBuf[size.length];
      int pos = 0;
      for (int i = 0; i < size.length; i++) {
        directAddrs[i] = allocator.directBuffer(size[i], size[i]);
        OutputStream out = new ByteBufOutputStream(directAddrs[i]);
        out.write(data, pos, size[i]);
        pos += size[i];
        out.close();
      }
    }
    long[] addrs = new long[size.length];
    for (int i = 0; i < size.length; i++) {
      addrs[i] = directAddrs[i].memoryAddress();
    }
    return addrs;
  }

  public void releaseDirectMemory() {
    if (directAddrs != null) {
      for (int i = 0; i < directAddrs.length; i++) {
        directAddrs[i].release();
      }
    }
  }

  public int getRefCnt() {
    if (directAddrs != null) {
      return directAddrs[0].refCnt();
    }
    return 0;
  }
}