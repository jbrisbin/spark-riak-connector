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
import org.apache.spark.api.java.JavaRDD
import scala.reflect.ClassTag

class RiakJavaRDD[R](override val rdd: RiakRDD[R], override val classTag: ClassTag[R]) extends JavaRDD[R](rdd)(classTag) {

  private def wrap(newRDD: RiakRDD[R]): RiakJavaRDD[R] = new RiakJavaRDD[R](newRDD, classTag)

  def query2iRange(index: String, from: Long, to: Long): RiakJavaRDD[R] = wrap(rdd.query2iRange(index, from, to))

  def query2iRangeLocal(index: String, from: Long, to: Long): RiakJavaRDD[R] = wrap(rdd.query2iRangeLocal(index, from, to))

  def query2iKeys[K](index: String, keys: K*): RiakJavaRDD[R] = wrap(rdd.query2iKeys(index, keys:_*))

  def queryBucketKeys(keys: String*): RiakJavaRDD[R] = wrap(rdd.queryBucketKeys(keys:_*))

  def queryAll: RiakJavaRDD[R] = wrap(rdd.queryAll())

  def partitionBy2iRanges[K](index: String, ranges: (K, K)*): RiakJavaRDD[R] = wrap(rdd.partitionBy2iRanges(index, ranges:_*))

  def partitionBy2iKeys[K](index: String, keys: K*): RiakJavaRDD[R] = wrap(rdd.partitionBy2iKeys(index, keys:_*))

}

object RiakJavaRDD {
  def apply[R](rdd: RiakRDD[R])(clazz: Class[R]): RiakJavaRDD[R] = new RiakJavaRDD[R](rdd, ClassTag(clazz))
  def apply[R](rdd: RiakRDD[R])(classTag: ClassTag[R]): RiakJavaRDD[R] = new RiakJavaRDD[R](rdd, classTag)
}
