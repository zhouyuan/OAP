package com.intel.sparkColumnarPlugin.vectorized

import org.apache.spark.internal.Logging
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.TaskContext

/**
 * An Iterator that insures that the batches [[ColumnarBatch]]s it iterates over are all closed
 * properly.
 */
class CloseableColumnBatchIterator(itr: Iterator[ColumnarBatch])
    extends Iterator[ColumnarBatch]
    with Logging {
  var cb: ColumnarBatch = null

  private def closeCurrentBatch(): Unit = {
    if (cb != null) {
      //logInfo(s"${itr} close ${cb}.")
      cb.close
      cb = null
    }
  }

  TaskContext
    .get()
    .addTaskCompletionListener[Unit]((tc: TaskContext) => {
      closeCurrentBatch()
    })

  override def hasNext: Boolean = {
    closeCurrentBatch()
    itr.hasNext
  }

  override def next(): ColumnarBatch = {
    closeCurrentBatch()
    cb = itr.next()
    cb
  }
}
