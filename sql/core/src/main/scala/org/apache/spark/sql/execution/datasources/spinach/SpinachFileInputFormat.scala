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

package org.apache.spark.sql.execution.datasources.spinach

import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConverters._

import com.google.common.base.Stopwatch
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs._
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

import org.apache.spark.SparkException
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

class SpinachFileInputFormat extends FileInputFormat[NullWritable, InternalRow] {
  private final val LOG: Log = LogFactory.getLog(classOf[SpinachFileInputFormat])

  def createRecordReader(split: InputSplit, context: TaskAttemptContext)
  : RecordReader[NullWritable, InternalRow] = {
    val s = split.asInstanceOf[FiberSplit]
    val conf = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
    val schema = StructType.fromString(conf.get(SpinachFileFormat.SPINACH_META_SCHEMA))
    val filterScanner = SpinachFileFormat.deserialzeFilterScanner(conf)
    val requiredIds = SpinachFileFormat.getRequiredColumnIds(conf)

    return new SpinachDataReader2(s.getFile, schema, filterScanner, requiredIds)
  }

  override def getSplits(job: JobContext): JList[InputSplit] = {
    val sw: Stopwatch = new Stopwatch().start

    val splits: JList[InputSplit] = new JArrayList[InputSplit]
    val jobConf = SparkHadoopUtil.get.getConfigurationFromJobContext(job)

    val spinachMeta = listStatus(job).asScala.find(file =>
      file.getPath.getName.endsWith(SpinachFileFormat.SPINACH_META_EXTENSION))
    val metaPath = spinachMeta match {
      case Some(f) => f.getPath
      case None => throw new SparkException("No Spinach meta file found for the job.")
    }
    val meta = DataSourceMeta.initialize(metaPath, jobConf)

    meta.fileMetas.foreach { fileMeta =>
      val path = new Path(metaPath.getParent, fileMeta.dataFileName)
      val fs = path.getFileSystem(jobConf)
      val file = fs.getFileStatus(path)
      val blkLocations = fs.getFileBlockLocations(file, 0, file.getLen)
      val cachedHosts = FiberSensor.getHosts(path.toString)
      val hosts = (cachedHosts.toBuffer ++ blkLocations(0).getHosts).distinct
      splits.add(new FiberSplit(file.getLen, path, hosts.toArray))
    }

    sw.stop
    if (LOG.isDebugEnabled) {
      LOG.debug(s"Total # of splits generated by getSplits: ${splits.size}, " +
        s"TimeTaken: ${sw.elapsedMillis}")
    }

    splits
  }

  protected override def isSplitable(context: JobContext, file: Path): Boolean = false
}
