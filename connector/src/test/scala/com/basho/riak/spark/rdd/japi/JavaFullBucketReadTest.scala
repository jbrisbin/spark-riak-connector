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

import com.basho.riak.spark.rdd.{SingleNodeRiakSparkTest, RiakCommonTests}
import org.apache.spark.SparkConf
import org.junit.Assert._
import org.junit.Test

import com.basho.riak.spark.japi.SparkJavaUtil._
import org.junit.experimental.categories.Category
import scala.collection.JavaConversions._

@Category(Array(classOf[RiakCommonTests]))
class JavaFullBucketReadTest extends SingleNodeRiakSparkTest with AbstractJavaTest {
  private val NumberOfTestValues = 20
  private val SplitSize = 10

  protected override val jsonData: Option[String] = Some({
    val data = for {
      i <- 1 to NumberOfTestValues
      data = Map("key"->s"k$i","value"->s"v$i", "indexes"->Map("creationNo"->i))
    } yield data

    asStrictJSON(data)
  })

  override protected def initSparkConf(): SparkConf = {
    val conf = super.initSparkConf()
    conf.set("spark.riak.input.split.count", SplitSize.toString)
    conf
  }

  /**
    * Utilize CoveragePlan support to perform local reads
    */
  @Test
  def fullBucketRead(): Unit = {
    val data = jsc.riakBucket(DefaultNamespace, classOf[String])
      .queryAll
      .mapPartitionsWithIndex(funcReMapWithPartitionIdx[String], preservesPartitioning = true)
      .mapToPair(pairFunc[String])

    val test = data.values().collect()
      .sortWith((l1, l2) => l1.substring(1).toInt < l2.substring(1).toInt).toArray
    val partitions = data.groupByKey().collect()

    assertEquals(SplitSize, partitions.length)

    // verify returned values
    for{i <- 1 to NumberOfTestValues} {
      assertEquals( "v" + i, test(i - 1))
    }
  }
}
