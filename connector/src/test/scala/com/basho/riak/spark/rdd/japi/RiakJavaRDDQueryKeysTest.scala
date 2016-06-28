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

import com.basho.riak.spark.japi.SparkJavaUtil._
import com.basho.riak.spark.rdd.{SingleNodeRiakSparkTest, RiakCommonTests}
import org.junit.Test
import org.junit.experimental.categories.Category

@Category(Array(classOf[RiakCommonTests]))
class RiakJavaRDDQueryKeysTest extends SingleNodeRiakSparkTest with AbstractJavaTest {

  protected override val jsonData: Option[String] =
    Some("[" +
      " { key: 'key-1', indexes: {creationNo: 1, category: 'neighbor'}, value: {user_id: 'u1', timestamp: '2014-11-24T13:14:04.823Z'}}" +
      ",{ key: 'key-2', indexes: {creationNo: 2, category: 'visitor'}, value:  {user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}}" +
      ",{ key: 'key-3', indexes: {creationNo: 3, category: 'neighbor'}, value: {user_id: 'u1', timestamp: '2014-11-24T13:18:04'}}" +
      ",{ key: 'key-4', indexes: {creationNo: 4, category: 'stranger'}, value: {user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}}" +
      ",{ key: 'key-5', indexes: {creationNo: 5, category: 'stranger'}, value: {user_id: 'u3', timestamp: '2014-11-24T13:16:04.823Z'}}" +
      ",{ key: 'key-6', indexes: {creationNo: 6, category: 'stranger'}, value: {user_id: 'u3', timestamp: '2014-11-24T13:21:04.825Z'}}" +
      ",{ key: 'key-7', indexes: {creationNo: 7, category: 'stranger'}, value: {user_id: 'u3', timestamp: '2014-11-24T12:01:04.825Z'}}" +
      "]")

  @Test
  def checkBucketKeysQuery: Unit = {
    val results = jsc.riakBucket(DefaultNamespace, classOf[java.util.Map[_, _]])
      .queryBucketKeys("key-1", "key-4", "key-6").collect
    assertEqualsUsingJSONIgnoreOrder("[" +
      "{timestamp: '2014-11-24T13:14:04.823Z', user_id: 'u1'}," +
      "{timestamp: '2014-11-24T13:14:04Z', user_id: 'u2'}," +
      "{timestamp: '2014-11-24T13:21:04.825Z', user_id: 'u3'}" + "]", results)
  }

  @Test
  def check2iRangeQueryPairRDD: Unit = {

    val results = jsc.riakBucket[String, Map[String, _]](DefaultNamespace, classOf[String], classOf[Map[String, _]])
      .queryBucketKeys("key-1", "key-4", "key-6").collect

    assertEqualsUsingJSONIgnoreOrder("[" +
      "  ['key-1', {timestamp: '2014-11-24T13:14:04.823Z', user_id: 'u1'}]" +
      ", ['key-4', {timestamp: '2014-11-24T13:14:04Z', user_id: 'u2'}]" +
      ", ['key-6', {timestamp: '2014-11-24T13:21:04.825Z', user_id: 'u3'}]" + "]", results)
  }

  @Test
  def check2iKeysQuery: Unit = {
    val results = jsc.riakBucket(DefaultNamespace, classOf[java.util.Map[_, _]])
      .query2iKeys("category", "stranger", "visitor").collect

    assertEqualsUsingJSONIgnoreOrder("[" +
      "{timestamp: '2014-11-24T13:15:04.824Z', user_id: 'u1'}" +
      ", {timestamp: '2014-11-24T13:14:04Z', user_id: 'u2'}" +
      ", {timestamp: '2014-11-24T13:16:04.823Z', user_id: 'u3'}" +
      ", {timestamp: '2014-11-24T13:21:04.825Z', user_id: 'u3'}" +
      ", {timestamp: '2014-11-24T12:01:04.825Z', user_id: 'u3'}" + "]", results)
  }

  @Test
  def check2iPartitionByKeys: Unit = {
    val data = jsc.riakBucket(DefaultNamespace, classOf[User])
      .partitionBy2iKeys("category", "neighbor", "visitor", "stranger")
      .mapPartitionsWithIndex(funcReMapWithPartitionIdx[User], preservesPartitioning = true)
      .mapToPair(pairFunc[User]).groupByKey.collect

    assertEqualsUsingJSONIgnoreOrder("[" +
      "[0, [" + " {user_id: 'u1', timestamp: '2014-11-24T13:14:04.823Z'}" +
      " ,{user_id: 'u1', timestamp: '2014-11-24T13:18:04'}]]" +
      ",[1, [" + " {user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}]]" +
      ",[2, [" + " {user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}" +
      " ,{user_id: 'u3', timestamp: '2014-11-24T13:16:04.823Z'}" +
      " ,{user_id: 'u3', timestamp: '2014-11-24T13:21:04.825Z'}" +
      " ,{user_id: 'u3', timestamp: '2014-11-24T12:01:04.825Z'}]]" +
      "]", data)
  }
}
