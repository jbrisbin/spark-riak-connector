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

import com.basho.riak.spark.rdd.RiakTSRDD
import org.apache.spark.api.java.JavaRDD

import scala.reflect.ClassTag

class RiakTSJavaRDD[R](override val rdd: RiakTSRDD[R], override val classTag: ClassTag[R]) extends JavaRDD[R](rdd)(classTag)  {

  private def wrap(newRDD: RiakTSRDD[R]): RiakTSJavaRDD[R] = new RiakTSJavaRDD[R](newRDD, classTag)

  def sql(queryString: String): RiakTSJavaRDD[R] = wrap(rdd.sql(queryString))

}

object RiakTSJavaRDD {
  def apply[R](rdd: RiakTSRDD[R], clazz: Class[R]): RiakTSJavaRDD[R] = new RiakTSJavaRDD[R](rdd, ClassTag(clazz))
  def apply[R](rdd: RiakTSRDD[R], classTag: ClassTag[R]): RiakTSJavaRDD[R] = new RiakTSJavaRDD[R](rdd, classTag)
}
