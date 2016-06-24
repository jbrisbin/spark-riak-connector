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
package com.basho.riak.spark.japi.rdd

import com.basho.riak.spark.rdd.RiakRDD
import org.apache.spark.api.java.JavaPairRDD

import scala.reflect.ClassTag

class RiakJavaPairRDD[K, V](override val rdd: RiakRDD[(K, V)], override val kClassTag: ClassTag[K], override val vClassTag: ClassTag[V])
  extends JavaPairRDD[K, V](rdd)(kClassTag, vClassTag) {

  def wrap(newRDD: RiakRDD[(K, V)]): RiakJavaPairRDD[K, V] = new RiakJavaPairRDD[K, V](newRDD, kClassTag, vClassTag)

  def query2iRange(index: String, from: Long, to: Long): RiakJavaPairRDD[K, V] = wrap(rdd.query2iRange(index, from, to))

  def queryBucketKeys(keys: String*): RiakJavaPairRDD[K, V] = wrap(rdd.queryBucketKeys(keys:_*))
}

object RiakJavaPairRDD {
  def apply[K, V](rdd: RiakRDD[(K, V)])
                 (kClass: Class[K], vClass: Class[V]): RiakJavaPairRDD[K, V] = new RiakJavaPairRDD[K, V](rdd, ClassTag(kClass), ClassTag(vClass))
  def apply[K, V](rdd: RiakRDD[(K, V)])
                 (kClassTag: ClassTag[K], vClassTag: ClassTag[V]): RiakJavaPairRDD[K, V] = new RiakJavaPairRDD[K, V](rdd, kClassTag, vClassTag)
}
