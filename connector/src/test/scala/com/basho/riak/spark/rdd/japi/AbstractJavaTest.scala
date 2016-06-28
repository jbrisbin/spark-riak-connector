/**
  * *****************************************************************************
  * Copyright (c) 2016 IBM Corp.
  *
  * Created by Basho Technologies for IBM
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  * *****************************************************************************
  */
package com.basho.riak.spark.rdd.japi

import java.io.Serializable

import com.basho.riak.client.core.query.{Namespace, Location, RiakObject}
import com.basho.riak.spark.rdd.{BucketDef, AbstractRiakSparkTest}
import com.basho.riak.spark.util.RiakObjectConversionUtil
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.{Function2, PairFunction}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._

import scala.reflect.ClassTag
import scala.runtime.AbstractFunction2

case class User (timestamp: String, user_id: String)

trait AbstractJavaTest extends AbstractRiakSparkTest {
  protected var jsc: JavaSparkContext = _
  protected val DefaultNamespace = new Namespace(BucketDef.DefaultBucketType,"test-bucket")
  protected val DefaultNamespace4Store = new Namespace(BucketDef.DefaultBucketType, "test-bucket-4store")
  protected var CreationIndex: String = "creationNo"

  override def createSparkContext(conf: SparkConf): SparkContext = {
    val sc: SparkContext = new SparkContext(conf)
    jsc = new JavaSparkContext(sc)
    sc
  }

  def convertRiakObject[T: ClassTag](l: Location, ro: RiakObject):T = RiakObjectConversionUtil.from(l, ro)


  protected class FuncMapTupleToJavaPairRdd[K, V] extends PairFunction[(K, V), K, V] with Serializable {
    @throws(classOf[Exception])
    def call(t: (K, V)): (K, V) = t
  }

  class PairConversionFunction[V](implicit ctV :ClassTag[V])
    extends AbstractFunction2[Location, RiakObject, (String,V)] with Serializable {

    def apply(l: Location, r: RiakObject): (String,V) = {
      val k = l.getKeyAsString
      k -> RiakObjectConversionUtil.from(l, r)
    }
  }

  def funcReMapWithPartitionIdx[T] = new Function2[Integer, java.util.Iterator[T], java.util.Iterator[(Integer, T)]] with Serializable {
    override def call(partitionIdx: Integer, iter: java.util.Iterator[T]): java.util.Iterator[(Integer, T)] = {
      iter.map(x => partitionIdx -> x)
    }
  }

  def pairFunc[T]: PairFunction[(Integer, T), Integer, T] = new PairFunction[(Integer, T), Integer, T] {
      def call(x: (Integer, T))  = new Tuple2(x._1, x._2)
  }
}


