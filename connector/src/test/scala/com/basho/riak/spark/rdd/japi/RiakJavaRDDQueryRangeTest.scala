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
import org.junit.Test
import org.junit.experimental.categories.Category
import com.basho.riak.spark.japi.SparkJavaUtil._

@Category(Array(classOf[RiakCommonTests]))
class RiakJavaRDDQueryRangeTest extends SingleNodeRiakSparkTest with AbstractJavaTest {

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
  def check2iRangeQuery(): Unit = {
    val results = jsc.riakBucket(DefaultNamespace, classOf[java.util.Map[_, _]])
    .query2iRange(CreationIndex, 2L, 4L).collect()

    assertEqualsUsingJSONIgnoreOrder("[" +
      " {user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}" +
      ",{user_id: 'u1', timestamp: '2014-11-24T13:18:04'}" +
      ",{user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}" +
      "]", results)
  }

  @Test
  def check2iRangeQueryPairRDD: Unit = {
    val results = jsc.riakBucket[String, Map[String, _]](DefaultNamespace, classOf[String], classOf[Map[String, _]])
      .query2iRange(CreationIndex, 2L, 4L).collect()

    assertEqualsUsingJSONIgnoreOrder("[" +
      " ['key-2', {user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}]" +
      ",['key-3', {user_id: 'u1', timestamp: '2014-11-24T13:18:04'}]" +
      ",['key-4', {user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}]" +
      "]",
      results)
  }

  @Test
  def check2iRangeLocalQuery: Unit = {
    val results = jsc.riakBucket(DefaultNamespace, classOf[Map[String, _]])
      .query2iRangeLocal(CreationIndex, 2L, 4L).collect()

    assertEqualsUsingJSONIgnoreOrder("[" +
      " {user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}" +
      ",{user_id: 'u1', timestamp: '2014-11-24T13:18:04'}" +
      ",{user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}" +
      "]",
      results)
  }

  @Test
  def check2iPartitionByIntegerKeyRanges(): Unit = {
    val data = jsc.riakBucket(DefaultNamespace, classOf[User])
      .partitionBy2iRanges(CreationIndex, (1, 3), (4, 6), (7, 12))
      .mapPartitionsWithIndex(funcReMapWithPartitionIdx[User], preservesPartitioning = true)
      .mapToPair(pairFunc[User])
      .groupByKey()
      .collect()

    assertEqualsUsingJSONIgnoreOrder("[" +
      // The 1st partition should contains first 3 item
      "[0, [" +
      "     {user_id: 'u1', timestamp: '2014-11-24T13:14:04.823Z'}" +
      "     ,{user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}" +
      "     ,{user_id: 'u1', timestamp: '2014-11-24T13:18:04'}" +
      "]]" +

      // The 2nd partition should contsins items; 4,5,6
      ",[1, [" +
      "     {user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}" +
      "     ,{user_id: 'u3', timestamp: '2014-11-24T13:16:04.823Z'}" +
      "     ,{user_id: 'u3', timestamp: '2014-11-24T13:21:04.825Z'}" +
      "]]" +

      // The 3rd partition should contains the only 7th item
      ",[2, [" +
      "     {user_id: 'u3', timestamp: '2014-11-24T12:01:04.825Z'}" +
      "]]" +
      "]",
      data)
  }

}
