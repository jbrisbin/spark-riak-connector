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
package com.basho.riak.spark.japi

import com.basho.riak.client.core.query.Namespace
import com.basho.riak.spark._
import com.basho.riak.spark.rdd.{BucketDef, RDDFunctions}
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.writer.mapper.DefaultWriteDataMapper
import com.basho.riak.spark.writer.ts.RowDef
import com.basho.riak.spark.writer.{ RiakWriter, WriteDataMapperFactory, WriteConf}
import org.apache.spark.rdd.RDD

class RDDJavaFunctions[T](rdd: RDD[T], rddFunctions: RDDFunctions[T]) {
  implicit def defaultValueWriterFactory[T]: WriteDataMapperFactory[T, KeyValue] = DefaultWriteDataMapper.factory
  val rddConf = rdd.context.getConf
  val defaultConnector = RiakConnector(rddConf)

  def saveToRiak(bucketName: String, bucketType: String): Unit = {
    saveToRiak(bucketName, bucketType, WriteConf(rddConf), defaultConnector, DefaultWriteDataMapper.factory)
  }

  def saveToRiak(bucketName: String): Unit = saveToRiak(bucketName, BucketDef.DefaultBucketType)

  def saveToRiak(ns: Namespace): Unit = saveToRiak(ns.getBucketNameAsString, ns.getBucketTypeAsString)

  def saveToRiak(bucketName: String, bucketType: String, writeConf: WriteConf, connector: RiakConnector,
                 vwf: WriteDataMapperFactory[T, (String, Any)]): Unit =
    new RDDFunctions[T](rdd).saveToRiak(bucketName, bucketType, writeConf)(connector, vwf)

  def saveToRiak(bucketName: String, vwf: WriteDataMapperFactory[T, (String, Any)]): Unit =
    new RDDFunctions[T](rdd).saveToRiak(bucketName, BucketDef.DefaultBucketType)(defaultConnector, vwf)

  def saveToRiak(ns: Namespace, vwf: WriteDataMapperFactory[T, (String, Any)]): Unit = new RDDFunctions[T](rdd).saveToRiak(ns)(vwf)

  def saveToRiakTS(connector: RiakConnector, bucketType: String, bucketName: String, writeConf: WriteConf, factory: WriteDataMapperFactory[T, RowDef]): Unit = {
    val writer = RiakWriter.tsWriter[T](connector, bucketType, bucketName, writeConf)(factory)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  def saveToRiakTS(bucketName: String): Unit = {
    val factory: WriteDataMapperFactory[T, RowDef] = WriteDataMapperFactory.sqlRowFactory.asInstanceOf[WriteDataMapperFactory[T, RowDef]]
    saveToRiakTS(defaultConnector, BucketDef.DefaultBucketType, bucketName, WriteConf(rddConf), factory)
  }
}

object RDDJavaFunctions {
  def apply[T](rdd: RDD[T]): RDDJavaFunctions[T] = new RDDJavaFunctions[T](rdd, new RDDFunctions[T](rdd))
}
