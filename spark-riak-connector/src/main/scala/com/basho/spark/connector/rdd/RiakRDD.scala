/**
 * Copyright (c) 2015 Basho Technologies, Inc.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.basho.spark.connector.rdd


import com.basho.riak.client.core.query.{Location, RiakObject}
import com.basho.spark.connector.query._
import com.basho.spark.connector.rdd.partitioner.{RiakLocalCoveragePartition, RiakCoveragePlanBasedPartitioner, RiakKeysPartition, RiakKeysPartitioner}

import scala.reflect.ClassTag
import scala.language.existentials

import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import com.basho.spark.connector.util.{DataConvertingIterator, CountingIterator}

class RiakRDD[R] private[connector] (
    @transient sc: SparkContext,
    val connector: RiakConnector,
    val bucketType: String,
    val bucketName: String,
    val convert:(Location, RiakObject) => R,
    val keys: Option[QueryData[_]] = None,
    val readConf: ReadConf = ReadConf()
    )(
      implicit val ct : ClassTag[R])
  extends RDD[R](sc, Seq.empty) with Logging {

  override def getPartitions: Array[Partition] = {

    val partitions = keys match{
      case None =>
        throw new IllegalMonitorStateException("Query criteria should be provided")

      case Some(rk) =>
        rk.coverageEntries match {
          case Some(ce) =>
            RiakCoveragePlanBasedPartitioner.partitions(connector, BucketDef(bucketType, bucketName), readConf, keys.get)

          case _ =>
            RiakKeysPartitioner.partitions(connector.hosts, keys.get)
        }
    }

    logDebug(s"Created total ${partitions.length} Spark partitions for bucket {$bucketType.$bucketName}.")
    if(isTraceEnabled()) {
      logTrace(s"partitions:\n\t${partitions.mkString("\n\t")}")
    }

    partitions
  }

  override def compute(split: Partition, context: TaskContext): Iterator[R] = {

    split match {
      case rp: RiakKeysPartition[_] =>
        val session = connector.openSession()
        val startTime = System.currentTimeMillis()

        val query = Query(BucketDef(bucketType, bucketName), readConf, rp.keys)

        val iterator: Iterator[(Location, RiakObject)] = new DataQueryingIterator(query, session, connector.minConnections)
        val convertingIterator = new DataConvertingIterator[R](iterator, convert)
        val countingIterator = new CountingIterator[R](convertingIterator)
        context.addTaskCompletionListener { (context) =>
          val endTime = System.currentTimeMillis()
          val duration = (endTime - startTime) / 1000.0
          logDebug(s"Fetched ${countingIterator.count} rows from ${query.bucket}" +
            f" for partition ${rp.index} in $duration%.3f s.")
          session.shutdown()
        }
        countingIterator

      // TODO: remove duplicated code
      case rl: RiakLocalCoveragePartition[_] =>
        val session = connector.openSession(Some(Seq(rl.primaryHost)))
        val startTime = System.currentTimeMillis()

        val query = Query(BucketDef(bucketType, bucketName), readConf, rl.queryData)
        val iterator: Iterator[(Location, RiakObject)] = new DataQueryingIterator(query, session, connector.minConnections)
        val convertingIterator = new DataConvertingIterator[R](iterator, convert)
        val countingIterator = new CountingIterator[R](convertingIterator)
        context.addTaskCompletionListener { (context) =>
          val endTime = System.currentTimeMillis()
          val duration = (endTime - startTime) / 1000.0
          logDebug(s"Fetched ${countingIterator.count} rows from ${query.bucket}" +
            f" for partition ${rl.index} in $duration%.3f s.")
          session.shutdown()
        }
        countingIterator

      case _ =>
        throw new IllegalStateException("Unsupported partition type")
    }
  }

  private def copy(
                   keys: Option[QueryData[_]] = keys,
                   readConf: ReadConf = readConf, connector: RiakConnector = connector): RiakRDD[R] =
    new RiakRDD(sc, connector, bucketType, bucketName, convert, keys, readConf)

  def query2iRange[K](index: String, from: K, to: K): RiakRDD[R] = {
    copy(keys = Some(QueryData.create2iKeyRanges[K](index, (from, Some(to)))))
  }

  def query2iKeys[K](index: String, keys: K* ): RiakRDD[R] = {
    copy(keys = Some(QueryData.create2iKeys[K](index, keys:_*)))
  }

  def query2iRangeLocal[K](index: String, from: K, to: K): RiakRDD[R] ={
    copy(keys = Some(QueryData.create2iKeyRangesLocal(index, (from, Some(to)))))
  }

  /**
   * Perform query all data from the bucket.
   * Utilizes Coverage Plan to perform bunch of local read
   *
   * @see RiakCoveragePlanBasedPartitioner
   */
  def queryAll(): RiakRDD[R] ={
    copy(keys = Some(QueryData.createReadLocal()))
  }

  def queryBucketKeys(keys: String*): RiakRDD[R] = {
    copy(keys = Some(QueryData.createBucketKeys(keys:_*)))
  }

  /**
   * Create separate partition for each 2i key range
   */
  def partitionBy2iRanges[K](index: String, ranges: (K, K)*): RiakRDD[R] = {
    val r = ranges map( x => (x._1, Some(x._2)) )
    copy(keys = Some(QueryData.create2iKeyRanges[K](index, r:_*)))
  }

  /**
   * Create separate partition for each key
   */
  def partitionBy2iKeys[K](index: String, keys: K*): RiakRDD[R] = {
    val r = keys map( k=> (k, None) )
    copy(keys = Some(QueryData.create2iKeyRanges[K](index, r:_*)))
  }
}

object RiakRDD {
  def apply[T](sc: SparkContext, bucketType: String, bucketName: String, convert: (Location, RiakObject) => T)
              (implicit ct: ClassTag[T]): RiakRDD[T] =
    new RiakRDD[T](
      sc, RiakConnector(sc.getConf), bucketType, bucketName, convert)

  def apply[K, V](sc: SparkContext, bucketType: String, bucketName: String, convert: (Location, RiakObject) => (K, V))
                 (implicit keyCT: ClassTag[K], valueCT: ClassTag[V]): RiakRDD[(K, V)] =
    new RiakRDD[(K, V)](
      sc, RiakConnector(sc.getConf), bucketType, bucketName, convert)
}
