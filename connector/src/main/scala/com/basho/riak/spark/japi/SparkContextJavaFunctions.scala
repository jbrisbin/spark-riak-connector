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

import java.io.Serializable
import com.basho.riak.client.core.query.{Namespace, RiakObject, Location}
import com.basho.riak.spark.japi.rdd.{RiakTSJavaRDD, RiakJavaPairRDD, RiakJavaRDD}
import com.basho.riak.spark.rdd._
import com.basho.riak.spark.rdd.connector.RiakConnector
import com.basho.riak.spark.rdd.mapper.ReadDataMapperFactory.DefaultReadDataMapper
import com.basho.riak.spark.rdd.mapper.{ReadPairValueDataMapper, ReadDataMapperFactory, ReadValueDataMapper}
import com.basho.riak.spark.util.JavaApiHelper._
import com.basho.riak.spark.util.RiakObjectConversionUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag
import scala.runtime.AbstractFunction2

class SparkContextJavaFunctions(@transient val sc: SparkContext) extends Serializable {

  class ConversionFunction[V](implicit classTag: ClassTag[V])
    extends AbstractFunction2[Location, RiakObject, V] with Serializable {

    def apply(l: Location, r: RiakObject): V = RiakObjectConversionUtil.from(l, r)(classTag)
  }

  def toJavaRDD[T](rdd: RiakRDD[T], targetClass: Class[T]): RiakJavaRDD[T] = new RiakJavaRDD[T](rdd, getClassTag(targetClass))
  def toJavaPairRDD[K, V](rdd: RiakRDD[(K, V)], keyClass: Class[K], valueClass: Class[V]): RiakJavaPairRDD[K, V] =
    new RiakJavaPairRDD[K, V](rdd, getClassTag(keyClass), getClassTag(valueClass))

  def riakBucket[T](bucketName: String, bucketType: String = BucketDef.DefaultBucketType, rdmf: ReadDataMapperFactory[T]): RiakJavaRDD[T] = {
    val rdd = RiakRDD[T](sc, bucketType, bucketName, None, ReadConf(sc.getConf))(getClassTag(rdmf.targetClass), rdmf)
    toJavaRDD(rdd, rdmf.targetClass)
  }

  def riakBucket[T](bucketName: String, bucketType: String, valueClass: Class[T]): RiakJavaRDD[T] = {
    val rdmf = ReadValueDataMapper.factory(getClassTag(valueClass))
    riakBucket(bucketName, bucketType, rdmf)
  }

  def riakBucket[T](ns: Namespace, rdmf: ReadDataMapperFactory[T]): RiakJavaRDD[T] = riakBucket(ns.getBucketNameAsString, ns.getBucketTypeAsString, rdmf)


  def riakBucket[T](ns: Namespace, valueClass: Class[T]): RiakJavaRDD[T] = riakBucket(ns.getBucketNameAsString, ns.getBucketTypeAsString, valueClass)


  def riakBucket[Tuple2[String, AnyRef]](ns: Namespace): RiakJavaRDD[(String, Any)] =
    riakBucket(ns.getBucketNameAsString, ns.getBucketTypeAsString)

  def riakBucket[Tuple2[String, AnyRef]](bucketName: String, bucketType: String): RiakJavaRDD[(String, Any)] =
    riakBucket(bucketName, bucketType, DefaultReadDataMapper)

  def riakBucket[K <: String, V](bucketName: String, bucketType: String,
                                 keyClass: Class[K], valueClass: Class[V], rdmf: ReadDataMapperFactory[(K, V)]): RiakJavaPairRDD[K, V] = {
    toJavaPairRDD(riakBucket(bucketName, bucketType, rdmf).rdd, keyClass, valueClass)
  }

  def riakBucket[K <: String, V](bucketName: String, bucketType: String,
                                 keyClass: Class[K], valueClass: Class[V]): RiakJavaPairRDD[K, V] = {
    val rdmf = ReadPairValueDataMapper.factory(getClassTag(keyClass), getClassTag(valueClass))
    riakBucket(bucketName, bucketType, keyClass, valueClass, rdmf)
  }

  def riakBucket[K <: String, V](ns: Namespace, keyClass: Class[K], valueClass: Class[V], rdmf: ReadDataMapperFactory[(K, V)]): RiakJavaPairRDD[K, V] = {
    riakBucket(ns.getBucketNameAsString, ns.getBucketTypeAsString, keyClass, valueClass, rdmf)
  }

  def riakBucket[K <: String, V](ns: Namespace, keyClass: Class[K], valueClass: Class[V]): RiakJavaPairRDD[K, V] = {
    riakBucket(ns.getBucketNameAsString, ns.getBucketTypeAsString, keyClass, valueClass)
  }

  def riakTSTable[T](bucketName: String, targetClass: Class[T]): RiakTSJavaRDD[T] = {
    val classTag: ClassTag[T] = getClassTag(targetClass)
    val rdd: RiakTSRDD[T] = RiakTSRDD(sc, bucketName, ReadConf(sc.getConf))(classTag, RiakConnector(sc.getConf))
    new RiakTSJavaRDD[T](rdd, classTag)
  }

  def riakTSTable[T](bucketName: String, schema: StructType, targetClass: Class[T]): RiakTSJavaRDD[T] = {
    val classTag: ClassTag[T] = getClassTag(targetClass)
    val rdd: RiakTSRDD[T] = RiakTSRDD(sc, bucketName, ReadConf(sc.getConf), Option(schema))(classTag, RiakConnector(sc.getConf))
    new RiakTSJavaRDD[T](rdd, classTag)
  }

}

